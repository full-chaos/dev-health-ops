package remaining

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The readiness gate, ported from _daily_metrics_ready
// (workers/recommendations_tasks.py:177).
//
// # THE RACE
//
// Go dispatches a daily-metrics run's repository partitions asynchronously and
// finalizes only once every one of them has succeeded. Recommendations
// evaluating before that read PARTIAL metric tables and persist misleading
// fired/tombstone rows for the day (CHAOS-2373). Nothing about that failure is
// loud: the rows are well-formed and merely wrong.
//
// # FAIL-OPEN IS A POLICY, NOT A JUDGEMENT THAT THE ERROR IS BENIGN
//
// A read error PROCEEDS. Fail-closed would wire an unknown gate-error rate
// directly to an org-wide recommendations wedge with no tombstones, which is
// the very outcome CHAOS-2373 is about. But proceeding is a decision about what
// the gate DOES, not a reclassification of the failure, so the error is still
// counted and still logged at ERROR level. Downgrading the log to WARNING would
// silently drop it from Sentry, whose LoggingIntegration only turns ERROR+ into
// events and which has no manual capture_exception call sites to fall back on.

// scheduledFanoutGenerationPrefix must equal
// scheduledFanoutGenerationPrefix.
//
// It is RE-DECLARED rather than imported because importing that package here is
// an import cycle: remaining -> daily -> daily/cicd -> remaining. A duplicated
// literal is exactly the drift hazard the chschema interpreter duplication
// caused in #2142, so it is pinned instead by an EXTERNAL test
// (package remaining_test), which can import both without closing the cycle.
//
// The drift would be silent and would defeat the gate entirely: a mismatched
// prefix finds no fan-out run, the gate reads that as "no evidence of partial
// data", and it proceeds on exactly the partial tables it exists to avoid.
const scheduledFanoutGenerationPrefix = "fixed-schedule:daily_metrics_fanout:"

// readinessGateStatusSucceeded is the one finalization status that proceeds.
const readinessGateStatusSucceeded = "succeeded"

// latestDailyMetricsRunSQL mirrors _LATEST_DAILY_METRICS_RUN_SQL verbatim in
// shape: the most recently created non-abandoned FAN-OUT generation for the
// org/day, plus whether any of its partitions is stuck.
//
// ORDER BY created_at DESC, generation DESC is the reference's tie-break and is
// kept: two generations created in the same instant would otherwise be resolved
// arbitrarily, and the gate would flip between runs on the same data.
const latestDailyMetricsRunSQL = `
    SELECT
        run.finalization_status,
        EXISTS (
            SELECT 1 FROM daily_metrics_partitions AS partition
            WHERE partition.run_id = run.id AND partition.status = 'failed_permanent'
        ) AS has_stuck_partition
    FROM daily_metrics_runs AS run
    WHERE run.org_id = CAST($1 AS uuid)
      AND run.target_day = CAST($2 AS date)
      AND starts_with(run.generation, $3)
      AND run.status NOT IN ('canceled', 'failed')
    ORDER BY run.created_at DESC, run.generation DESC
    LIMIT 1
`

// ReadinessObserver receives the gate's two positive signals.
//
// Both exist because the gate's interesting outcomes are SILENT by default: a
// skipped org writes no rows, and no rows is indistinguishable from a day on
// which no team tripped a rule -- which for this family is an ordinary result.
type ReadinessObserver interface {
	// RecommendationsReadinessFailOpen reports a gate read that failed and was
	// treated as ready anyway, labelled by error type.
	RecommendationsReadinessFailOpen(errorType string)
	// RecommendationsReadinessSkipped reports an org/day deliberately skipped
	// because the daily run is demonstrably unfinished.
	RecommendationsReadinessSkipped()
}

// ReadinessLogger is the narrow logging capability the gate needs.
type ReadinessLogger interface {
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}

// DailyMetricsReady reports whether Go's daily metrics for org/day are
// finished, and therefore whether recommendations may evaluate.
//
// Returns false ONLY when a fan-out run exists and is demonstrably unfinished.
// Every other outcome proceeds, each for its own reason:
//
//	org_id == "default"  daily_metrics_runs.org_id is typed uuid, so the
//	                     single-tenant sentinel is unrepresentable there by
//	                     construction -- there is no row to find, and treating
//	                     that as "unfinished" would wedge single-tenant forever.
//	no row               absence is not evidence of partial data. Go recorded
//	                     no org-wide run for this org/day; blocking here would
//	                     stop recommendations for every org whose day Go never
//	                     dispatched.
//	status succeeded     the day is durably complete for the whole org.
//	read error           fail-open, counted and logged at ERROR (see above).
func DailyMetricsReady(
	ctx context.Context,
	pool *pgxpool.Pool,
	orgID string,
	day time.Time,
	observer ReadinessObserver,
	logger ReadinessLogger,
) bool {
	if orgID == "default" {
		return true
	}

	var finalizationStatus *string
	var hasStuckPartition bool

	row := pool.QueryRow(ctx, latestDailyMetricsRunSQL,
		orgID, day.Format("2006-01-02"), scheduledFanoutGenerationPrefix)
	switch err := row.Scan(&finalizationStatus, &hasStuckPartition); {
	case err == nil:
		// Fall through to the decision below.
	case isNoRows(err):
		// No fan-out run for this org/day. Proceed -- see the doc comment.
		return true
	default:
		if observer != nil {
			observer.RecommendationsReadinessFailOpen(errorTypeLabel(err))
		}
		if logger != nil {
			// ERROR, not WARN. Proceeding is a policy choice about what the
			// gate does; it is not a claim that this read failure is benign,
			// and ERROR is the only level that reaches Sentry here.
			logger.Error(
				"failed to read daily metrics readiness state; treating as ready "+
					"(fail-open, CHAOS-4073 item 2)",
				"org_id", orgID, "day", day.Format("2006-01-02"), "error", err)
		}
		return true
	}

	if finalizationStatus == nil || *finalizationStatus == readinessGateStatusSucceeded {
		return true
	}

	if hasStuckPartition && logger != nil {
		// DIAGNOSTIC ONLY -- deliberately not part of the decision. The
		// existing contract ("any non-succeeded state -> skip") already covers
		// this run correctly; a failed_permanent partition only explains WHY it
		// may never resolve without a human repair call, which would otherwise
		// need someone running SQL against daily_metrics_partitions to discover
		// (CHAOS-4319). Folding it into the return value would change behaviour
		// while appearing to add an explanation.
		logger.Warn(
			"daily metrics fan-out run has a failed_permanent partition; it will not "+
				"finalize without a human /metric-executions/v1/{id}/repair call",
			"org_id", orgID, "day", day.Format("2006-01-02"))
	}
	if observer != nil {
		observer.RecommendationsReadinessSkipped()
	}
	return false
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// errorTypeLabel keeps the fail-open counter's label bounded.
//
// Python labels by exception CLASS name, which is a small closed set. Go error
// values are not, so the label is derived from the concrete type rather than
// the message -- a message would carry org ids and query text straight into
// label cardinality.
func errorTypeLabel(err error) string {
	if err == nil {
		return "none"
	}
	return fmt.Sprintf("%T", err)
}

// ScheduledFanoutGenerationPrefixForTest exposes the gate's prefix to the
// external test that pins it against the producer's. Exported for that single
// purpose: the alternative is an unpinned duplicated literal.
const ScheduledFanoutGenerationPrefixForTest = scheduledFanoutGenerationPrefix
