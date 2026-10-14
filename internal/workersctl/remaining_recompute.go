package workersctl

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// recomputeUsage is the --recompute help text of `metrics remaining start`.
const recomputeUsage = "rewrite the COVERED closed days of [--day, --to] after a basis change (needs --basis and an explicit --to; at most 31 days; the window must end before today UTC). " +
	"Eligible: a day that a succeeded partition of the family already covers. Not eligible, reported not_covered and left alone: a day with no succeeded run with output (run `start` without --recompute for it; for the daily families partition-recompute only redrives days that have a succeeded scheduled fan-out run). " +
	"A day with an automatic run still pending or running is reported in_progress and left alone. Runs only on the go-heavy pod (operator_backend_unavailable elsewhere). Idempotent per basis and day. Use --dry-run first"

// remainingRecomputeRequest is the validated input of `metrics remaining
// start --recompute`.
type remainingRecomputeRequest struct {
	family, org, fromRaw, toRaw, basis, reviewEvidence string
	from, to                                           time.Time
	dryRun                                             bool
	mutation                                           mutationFlags
}

// dispatchMetricsRemainingRecompute is the explicit recompute path of a basis
// change (CHAOS-9674): it rewrites days a remaining run already covers, which
// plain `start` refuses. It audits like `start` (ActionMetricsRemainingStart:
// the audit action set is a database constraint, so the reason code and
// correlation id tell a recompute from a start) and a --dry-run writes no
// audit row.
func dispatchMetricsRemainingRecompute(
	ctx context.Context, runtime *operatorRuntime, stdout, stderr io.Writer, req remainingRecomputeRequest,
) int {
	perform := func(ctx context.Context) int {
		if runtime.pools == nil || (!req.dryRun && runtime.registry == nil) {
			return writeError(stderr, "operator_backend_unavailable")
		}
		store, err := remaining.NewPostgresStore(runtime.pools.Domain)
		if err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		var publisher remaining.PartitionPublisher
		if !req.dryRun {
			publisher, err = remaining.NewPostgresPublisher(runtime.pools.Domain, runtime.registry)
			if err != nil {
				return writeError(stderr, "operator_backend_unavailable")
			}
		}
		var results []manualBackfillDayResult
		for cursor := req.from; !cursor.After(req.to); cursor = cursor.AddDate(0, 0, 1) {
			day := cursor.Format("2006-01-02")
			var outcome remaining.ManualRecomputeOutcome
			if req.dryRun {
				outcome, err = store.PreviewManualRecompute(ctx, req.family, req.org, day, req.basis)
			} else {
				outcome, err = store.StartManualRecomputeRun(ctx, req.family, req.org, day, req.basis, publisher)
			}
			if err != nil {
				results = append(results, manualBackfillDayResult{Day: day, Status: "error", Error: err.Error()})
				continue
			}
			results = append(results, manualBackfillDayResult{
				Day: day, Status: string(outcome.Disposition), RunID: outcome.RunID,
				PartitionID: outcome.PartitionID, Generation: outcome.Generation,
			})
		}
		hadDayError := anyManualBackfillDayErrored(results)
		code := writeResult(stdout, stderr, map[string]any{
			"recompute":       true,
			"dry_run":         req.dryRun,
			"family":          req.family,
			"org":             req.org,
			"basis":           req.basis,
			"generation":      remaining.ManualRecomputeGeneration(req.basis, req.family, req.org),
			"review_evidence": req.reviewEvidence,
			"days":            results,
			"note": "not_covered days were not touched: run `start` without --recompute for them. " +
				"in_progress days were not touched: rerun when the automatic run has finished. " +
				"already_recomputed days are done for this basis.",
			"readback_hint": fmt.Sprintf(
				"ClickHouse: SELECT day, count(), max(computed_at) FROM %s WHERE org_id = '%s' AND day BETWEEN '%s' AND '%s' GROUP BY day ORDER BY day",
				manualBackfillReadbackTable[req.family], req.org, req.fromRaw, req.toRaw,
			),
		})
		if code == 0 && hadDayError {
			return 1
		}
		return code
	}
	if req.dryRun {
		return dryRunPreview(ctx, runtime, stderr, joboperator.ActionMetricsRemainingStart, "organization", req.org, perform)
	}
	return auditedWrite(ctx, runtime, stderr, req.mutation, joboperator.ActionMetricsRemainingStart, "organization", req.org, perform)
}
