package daily

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/benchmarking"
)

// BenchmarkingFinalizeExecutor is the FINALIZE-SCOPE native implementation of
// the benchmarking metrics.daily family (CHAOS-4288 done via #2235/#2259;
// relocated here for CHAOS-5194, astra design-review finding F3).
//
// # WHY THIS MOVED FROM PARTITION SCOPE
//
// The prior BenchmarkingExecutor (removed by this change, along with
// anchorFromDiscoveredSet and containsRepository) ran once per org/day via an
// "anchor partition" trick: only the partition holding the org's
// lexicographically-first repo id computed benchmarking; every other
// partition no-op'd. That fixed DUPLICATION (an org with N repos writing N
// identical row sets, #2235/#2259) but not a race F3 identified: the anchor
// partition's OWN post_bridge phase can complete BEFORE every OTHER
// partition for the SAME org/day has written its own repo_metrics_daily /
// testops_*_metrics_daily / dora_metrics_daily rows -- benchmarking's actual
// cross-repo inputs (see benchmarking/clickhouse.go's Table list and
// benchmarking/run.go's DefaultBenchmarkMetrics). "First partition to
// finish" is not "last partition to finish," and benchmarking needs the
// latter -- it aggregates across every repo in the org, not just the
// anchor's own.
//
// Finalize scope is claimed ONLY once every one of the run's
// daily_metrics_partitions rows reaches status='succeeded'
// (PostgresStore.ClaimFinalize's own `NOT EXISTS (... status <> 'succeeded')`
// gate, postgres.go) -- moving here removes BOTH the anchor-partition
// mechanism (no longer needed: finalize runs exactly once per run by
// construction, so there is nothing left to anchor) AND the race, in the
// same change.
//
// Python had the IDENTICAL bug at the time (run_benchmarking_for_day was
// called from run_daily_metrics_job, once per partition, never from
// run_daily_metrics_finalize); this move did not touch Python's own compute
// path at the time -- it only moved WHEN the Go executor ran and, via the
// "benchmarking" skip_families gate in run_daily_metrics_finalize (this
// family's name is UNCHANGED from its partition-scope name -- this was a
// RELOCATION of the same family to a different seam, not a new family),
// suppressed Python's per-partition call so it no longer fired at all when
// the native executor was registered. CHAOS-4288 has since deleted
// run_benchmarking_for_day (and the rest of
// src/dev_health_ops/metrics/benchmarking/) entirely -- there is no more
// Python compute path or skip_families gate to speak of; this executor is
// the only computer left.
//
// # BELT AND BRACES: THIS EXECUTOR DOES NOT TRUST THE CALLER'S GATE BLINDLY
//
// ClaimFinalize's own gate is a real, transactional guarantee -- but per the
// "always add telemetry" rule and this codebase's standing discipline of
// never trusting an upstream invariant silently (the same posture CHAOS-5141
// takes for its own repoNamesByID gate), this executor VERIFIES the barrier
// directly via Store.PartitionCompletionCounts before computing anything,
// logs what it saw (total vs succeeded) on EVERY run, and REFUSES with a
// logged reason if any partition is not yet succeeded -- so a future change
// to ClaimFinalize's gating logic that silently regresses the guarantee is
// caught here too, not just trusted. This is CHAOS-4290's finalize-scope
// NO-FAIL-OPEN policy applied to a NEW failure mode this family's move
// introduces: the refusal returns an error (never (0, nil)), which
// retryCompatibilityError's default arm treats as Retryable, so the run
// redrives rather than silently completing with benchmarking skipped.
type BenchmarkingFinalizeExecutor struct {
	store  Store
	conn   driver.Conn
	writer *benchmarking.Writer
	nowUTC func() time.Time
	logger *slog.Logger
}

var errBenchmarkingFinalizeUnavailable = fmt.Errorf("benchmarking finalize native executor unavailable")

// ErrBenchmarkingPartitionsIncomplete is returned when this executor's own
// partition-completion check finds a partition that has not yet succeeded --
// see the package doc comment above for why this is checked here even though
// ClaimFinalize already checks it.
var ErrBenchmarkingPartitionsIncomplete = errors.New(
	"daily: benchmarking finalize ran while a partition was not yet succeeded")

// BenchmarkingFamilyName re-exports the single source of truth for this
// family's skip_families key. UNCHANGED from its partition-scope name
// ("benchmarking") -- this is a RELOCATION of the same family to a different
// seam, not a new family, unlike compounding_risk_team (CHAOS-5084), which
// really is a second, scope-distinct writer sharing a table with an existing
// repo-scope family. Asserted against pythonRecognisedFinalizeFamilies
// (daily.go) and job_daily.py's gate line (finalize_family_gate_agreement_test.go).
const BenchmarkingFamilyName = "benchmarking"

// NewBenchmarkingFinalizeExecutor fails closed on a nil store or connection,
// matching every other native family's construction-time policy. The store
// dependency is NEW relative to the removed partition-scope executor: this
// is the first native family in this codebase to need Postgres as well as
// ClickHouse, because the partition-completion barrier it verifies lives in
// Postgres (daily_metrics_partitions), not ClickHouse.
func NewBenchmarkingFinalizeExecutor(store Store, conn driver.Conn, logger *slog.Logger) (*BenchmarkingFinalizeExecutor, error) {
	if store == nil || conn == nil {
		return nil, errBenchmarkingFinalizeUnavailable
	}
	writer, err := benchmarking.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errBenchmarkingFinalizeUnavailable, err)
	}
	return &BenchmarkingFinalizeExecutor{
		store: store, conn: conn, writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
		logger: logger,
	}, nil
}

// ComputeFinalizeFamily implements daily.NativeFinalizeFamilyExecutor.
//
// The partition-completion check happens BEFORE any ClickHouse read --
// deliberately: a refusal here must never touch the family's actual inputs,
// so a caller cannot mistake a barrier failure for "computed on incomplete
// data" (there is no data-touching step to have run yet).
func (executor *BenchmarkingFinalizeExecutor) ComputeFinalizeFamily(
	ctx context.Context, run Run,
) (int, error) {
	if executor == nil || executor.store == nil || executor.conn == nil || executor.writer == nil {
		return 0, errBenchmarkingFinalizeUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}

	logger := executor.logger
	if logger == nil {
		logger = slog.Default()
	}

	total, succeeded, err := executor.store.PartitionCompletionCounts(ctx, run.ID)
	if err != nil {
		// #2277 r1 F2 (P2, confirmed): a lookup failure used to return before
		// ANY barrier log line was ever written, so an operator scanning this
		// family's own "benchmarking finalize partition barrier" log line for
		// a run that never computed would find nothing at all -- indistinguishable
		// from the run never having been claimed. Log the failure itself, with
		// the same identifiers the success-path Info line below carries, so the
		// two paths are equally visible.
		logger.Error("benchmarking finalize partition barrier lookup failed",
			"run_id", run.ID, "organization_id", run.OrganizationID,
			"target_day", run.TargetDay.Format("2006-01-02"),
			"error", err,
		)
		return 0, fmt.Errorf("%w: %v", errBenchmarkingFinalizeUnavailable, err)
	}
	logger.Info("benchmarking finalize partition barrier",
		"run_id", run.ID, "organization_id", run.OrganizationID,
		"target_day", run.TargetDay.Format("2006-01-02"),
		"partitions_total", total, "partitions_succeeded", succeeded,
	)
	if total == 0 {
		// A finalize job for a run with ZERO partitions is a caller bug, not
		// a legitimate "nothing to do" -- ClaimFinalize's `NOT EXISTS` check
		// is VACUOUSLY true when no partition rows exist at all, so a run
		// that was never given partitions could otherwise reach here and
		// silently no-op. Matches the removed anchor mechanism's own
		// "empty is an error" discipline (anchorFromDiscoveredSet).
		return 0, fmt.Errorf(
			"%w: run %s has zero partitions -- a finalize job should not exist for a run with no partitions",
			ErrInvalidState, run.ID)
	}
	if succeeded != total {
		logger.Error("benchmarking finalize refused: a partition is still open",
			"run_id", run.ID, "organization_id", run.OrganizationID,
			"target_day", run.TargetDay.Format("2006-01-02"),
			"partitions_total", total, "partitions_succeeded", succeeded,
		)
		return 0, fmt.Errorf("%w: run %s has %d/%d partitions succeeded",
			ErrBenchmarkingPartitionsIncomplete, run.ID, succeeded, total)
	}

	targetDay := run.TargetDay.UTC()
	asOfDay := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)

	loader, err := benchmarking.NewClickHouseLoader(executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	outputs, err := benchmarking.ComputeBenchmarkingForDay(
		ctx, loader, asOfDay, computedAt, run.OrganizationID,
		benchmarking.DefaultBenchmarkMetrics,
		benchmarking.DefaultCorrelationPairs,
		logger,
	)
	if err != nil {
		// Only insight generation propagates; every per-metric failure was
		// already swallowed inside, matching Python.
		return 0, err
	}

	// Forwarded finding, #2276 r2 F2 (P1, verified by
	// lane-ci-required-to-arc): a swallowed fetch failure inside
	// ComputeBenchmarkingForDay logs at the per-slice level (now with
	// org_id/day identifiers -- see Outputs.FetchFailures' own doc
	// comment), but nothing previously surfaced the AGGREGATE at the
	// finalize layer, so an operator watching this family's own log lines
	// (not grepping every per-metric warning) had no visibility into a run
	// degrading over time. Logged at Warn, not Error: Python's own
	// semantics are preserved (fail open per-metric, never abort the run),
	// so a nonzero count here is diagnostic, not a family failure -- the
	// row count this function still returns is what determines
	// success/refused/partial_write.
	if outputs.FetchFailures > 0 {
		logger.Warn("benchmarking finalize had swallowed fetch failures",
			"run_id", run.ID, "organization_id", run.OrganizationID,
			"target_day", run.TargetDay.Format("2006-01-02"),
			"fetch_failures", outputs.FetchFailures,
		)
	}

	// THE COUNT SURVIVES THE ERROR (preserved from the removed
	// BenchmarkingExecutor): WriteOutputs returns the TRUE number of rows
	// already on disk when it fails after a batch has landed, so the
	// family's partial_write row count stays accurate rather than
	// collapsing to 0 -- the one number an operator needs to judge how much
	// duplication a re-drive would create.
	return executor.writer.WriteOutputs(ctx, outputs, run.OrganizationID)
}

var _ NativeFinalizeFamilyExecutor = (*BenchmarkingFinalizeExecutor)(nil)
