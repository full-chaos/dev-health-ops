//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

// CHAOS-4405: run_daily_metrics_finalize now writes
// compounding_risk_daily(scope='team') and team_cognitive_load_daily
// (CHAOS-4399, #1963), exactly once per org/day, from finalize -- never from
// the per-partition path. A day finalized BEFORE that landed has zero rows
// in either table and needs finalize re-run purely to backfill them, even
// though its run already reached status='succeeded'. Both Go's
// ClaimFinalize and the Python compat bridge's own claim-row query
// hard-require status='running' before Finalize() can run at all, so
// RedriveFinalizeForRange must transiently reset an already-'succeeded' run
// back to a claimable state before it can be redriven at all -- the tests
// below prove that round-trip, its includeSucceeded gate, that a day with no
// eligible run is reported (not silently skipped), and team-lead's five
// approval conditions on the reset design: durable provenance (1), the
// CHAOS-4389 sweep permanently stepping back from any run this verb has
// touched regardless of outcome (2)/(3), a --dry-run preview that writes
// nothing (4), and a distinct telemetry/outcome label for the reset case (5).

const testFinalizeRedriveReason = "CHAOS-4405 test: backfilling team-scope aggregates"

// TestRedriveFinalizeForRangeResetsAndRedrivesAnAlreadySucceededDay is the
// primary CHAOS-4405 proof: a run that already reached status='succeeded'
// (the ordinary historical shape -- finalize ran fine, just before #1963's
// team-aggregation write existed) is NOT reachable through the ordinary
// ClaimFinalize path (its own guard: `if status != "running" { return nil,
// nil }`). RedriveFinalizeForRange must reset it back to
// status='running'/finalization_status='pending' in the same transaction as
// the publish, so a subsequent FinalizeHandler.Work execution (simulated
// here via processFinalizeJob) can reach it and land the run back on
// 'succeeded'. Also covers approval condition (1): a provenance row must
// exist, recording the prior state, actor, and reason, before the reset.
func TestRedriveFinalizeForRangeResetsAndRedrivesAnAlreadySucceededDay(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000002001"
		runID       = "00000000-0000-4000-8000-000000002002"
		partitionID = "00000000-0000-4000-8000-000000002003"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	// Simulate a run that already finalized successfully, historically --
	// long before this backfill tool ever runs.
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}
	var originalGeneration string
	if err := pool.QueryRow(ctx, `SELECT generation FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&originalGeneration); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-1", true, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" ||
		outcome.Days[0].RunID != runID || !outcome.Days[0].ResetFromSucceeded {
		t.Fatalf("RedriveFinalizeForRange outcome = %#v, want one redriven_reset_from_succeeded day for %s", outcome, runID)
	}

	// The reset must be durable and visible before the redriven job is
	// processed: this is what makes the run reachable via ClaimFinalize
	// again at all.
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")

	// Approval condition (1): a provenance row exists, written BEFORE the
	// reset, recording the run's exact prior state.
	var priorStatus, priorFinalizationStatus, actor, reason, nonce string
	if err := pool.QueryRow(ctx, `
SELECT prior_status, prior_finalization_status, actor, reason, nonce
FROM daily_metrics_finalize_redrive_events WHERE run_id = $1::uuid`, runID).
		Scan(&priorStatus, &priorFinalizationStatus, &actor, &reason, &nonce); err != nil {
		t.Fatalf("provenance row missing: %v", err)
	}
	if priorStatus != "succeeded" || priorFinalizationStatus != "succeeded" {
		t.Fatalf("provenance prior state = %s/%s, want succeeded/succeeded", priorStatus, priorFinalizationStatus)
	}
	if actor != "finalize-redrive" {
		t.Fatalf("provenance actor = %q, want finalize-redrive", actor)
	}
	if reason != testFinalizeRedriveReason {
		t.Fatalf("provenance reason = %q, want %q", reason, testFinalizeRedriveReason)
	}
	if nonce != "range-nonce-1" {
		t.Fatalf("provenance nonce = %q, want range-nonce-1", nonce)
	}

	// CHAOS-4405 finding (posted on #1971): the reset must give the run a
	// FRESH generation, never the original one -- the Python compatibility
	// bridge's execution-ledger identity is uuid5(run_id, family,
	// generation, scope_digest), and the ORIGINAL identity already reached
	// 'succeeded' the first time this run finalized. An unchanged
	// generation would make the redriven attempt hit the identical
	// identity and get silently skipped, never actually re-running
	// run_daily_metrics_finalize (see CHAOS-4409's
	// test_finalize_execution_is_skipped_not_reexecuted_for_the_same_identity_after_reclaim
	// for the Python-side proof of that exact contract).
	var generationAfterReset string
	if err := pool.QueryRow(ctx, `SELECT generation FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&generationAfterReset); err != nil {
		t.Fatal(err)
	}
	if generationAfterReset == originalGeneration {
		t.Fatalf("generation after reset = %q, want a value different from the original %q", generationAfterReset, originalGeneration)
	}

	var redriveOutboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key = $1`,
		"metrics.daily_finalize:redrive:"+runID+":range-nonce-1").Scan(&redriveOutboxCount); err != nil {
		t.Fatal(err)
	}
	if redriveOutboxCount != 1 {
		t.Fatalf("redrive finalize outbox rows = %d, want 1", redriveOutboxCount)
	}

	// Simulate the redriven job's execution reaching FinalizeHandler.Work --
	// the run lands back on 'succeeded'.
	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")

	// Team-lead escalation on conditions (2)/(3): CompleteFinalize must
	// close this run's OPEN provenance row as 'closed_succeeded' the
	// instant this specific claim's success commits -- an audit-trail row
	// left 'open' forever after its run visibly succeeded would misreport
	// "still in flight".
	var eventStatus string
	var closedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, closed_at FROM daily_metrics_finalize_redrive_events
WHERE run_id = $1::uuid ORDER BY created_at LIMIT 1`, runID).
		Scan(&eventStatus, &closedAt); err != nil {
		t.Fatal(err)
	}
	if eventStatus != "closed_succeeded" || closedAt == nil {
		t.Fatalf("redrive event after CompleteFinalize = status=%q closed_at=%v, want closed_succeeded with a timestamp", eventStatus, closedAt)
	}

	// Safe to run again for the SAME day (matches the ticket's own
	// idempotency proof at the Python level): a fresh nonce redrives it
	// again rather than erroring, and appends a SECOND provenance row
	// (append-only, one per invocation).
	repeatOutcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-2", true, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("repeat RedriveFinalizeForRange: %v", err)
	}
	if len(repeatOutcome.Days) != 1 || repeatOutcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("repeat RedriveFinalizeForRange outcome = %#v, want redriven_reset_from_succeeded again", repeatOutcome)
	}
	var provenanceRowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_finalize_redrive_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceRowCount); err != nil {
		t.Fatal(err)
	}
	if provenanceRowCount != 2 {
		t.Fatalf("provenance rows after 2 invocations = %d, want 2 (append-only)", provenanceRowCount)
	}
	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
}

// TestRedriveFinalizeForRangeSkipsSucceededDayWhenIncludeSucceededIsFalse
// proves the safe-subset mode: with includeSucceeded=false, an
// already-'succeeded' day is reported as skipped and its durable state is
// left completely untouched (no reset, no provenance row, no outbox row).
func TestRedriveFinalizeForRangeSkipsSucceededDayWhenIncludeSucceededIsFalse(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000002101"
		runID       = "00000000-0000-4000-8000-000000002102"
		partitionID = "00000000-0000-4000-8000-000000002103"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-dry", false, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "skipped_ineligible" || outcome.Days[0].RunID != runID {
		t.Fatalf("RedriveFinalizeForRange(includeSucceeded=false) = %#v, want one skipped_ineligible day for %s", outcome, runID)
	}
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")

	var outboxCount, provenanceCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key LIKE $1`,
		"metrics.daily_finalize:redrive:"+runID+"%").Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 0 {
		t.Fatalf("outbox rows after a skipped succeeded day = %d, want 0", outboxCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_finalize_redrive_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceCount); err != nil {
		t.Fatal(err)
	}
	if provenanceCount != 0 {
		t.Fatalf("provenance rows after a skipped succeeded day = %d, want 0", provenanceCount)
	}
}

// TestRedriveFinalizeForRangeReportsDaysWithNoEligibleRun covers both
// "shapes" of ineligibility across a multi-day range: a day with no run at
// all (never dispatched, or aged out), and a day whose run exists but does
// not have every partition succeeded. Both must be reported as
// skipped_ineligible, distinguishable by an empty RunID for the
// no-run-at-all case, so an operator reading the result can tell "nothing
// to redrive here" apart from "found something, chose not to touch it".
func TestRedriveFinalizeForRangeReportsDaysWithNoEligibleRun(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID           = "00000000-0000-4000-8000-000000002201"
		incompleteRunID = "00000000-0000-4000-8000-000000002202"
		incompletePID   = "00000000-0000-4000-8000-000000002203"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	day1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC) // no run at all
	day2 := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC) // run exists, partition still pending
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, incompleteRunID, orgID, day2, now)
	insertFinalizeTestPartition(t, ctx, pool, incompletePID, incompleteRunID, 0, "pending", now)

	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, day1, day2, "range-nonce-gap", true, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 2 {
		t.Fatalf("RedriveFinalizeForRange days = %d, want 2 (one per calendar day in range)", len(outcome.Days))
	}
	if outcome.Days[0].Outcome != "skipped_ineligible" || outcome.Days[0].RunID != "" {
		t.Fatalf("day1 (no run at all) = %#v, want skipped_ineligible with empty RunID", outcome.Days[0])
	}
	if outcome.Days[1].Outcome != "skipped_ineligible" {
		t.Fatalf("day2 (incomplete partitions) = %#v, want skipped_ineligible", outcome.Days[1])
	}
}

// TestFindStrandedFinalizeRunsExcludesAnyRunFinalizeRedriveHasTouched is
// team-lead's approval conditions (2) and (3), red-first: while
// finalize-redrive's own redriven finalize is plausibly still in flight, an
// unattended `daily-finalize --all-complete` sweep running concurrently or
// afterward must never pick up that run (or double-dispatch it). This test
// exercises the "job never even claimed" shape specifically: the redriven
// outbox job is discarded before ClaimFinalize is ever called for it
// (simulated the same way the original CHAOS-4389 red-first proof does --
// the outbox row is marked 'dead'), leaving the run
// status='running'/finalization_status='pending' forever with NO completion
// path ever firing to close its redrive event. That event therefore stays
// 'open' indefinitely and the exclusion is (by design, and disclosed) as
// permanent as the original stranding itself for this specific sub-case --
// an operator can always still recover it explicitly via
// `daily-finalize --run <id>`, which never consults this table at all. See
// TestFindStrandedFinalizeRunsReincludesARunAfterItsRedrivenFinalizeFails
// for the DIFFERENT, and more common, shape team-lead's escalation actually
// targeted: a redriven finalize that IS claimed and then fails.
func TestFindStrandedFinalizeRunsExcludesAnyRunFinalizeRedriveHasTouched(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000002301"
		runID       = "00000000-0000-4000-8000-000000002302"
		partitionID = "00000000-0000-4000-8000-000000002303"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-fail", true, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("RedriveFinalizeForRange outcome = %#v, want redriven_reset_from_succeeded", outcome)
	}
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")

	// Simulate the redriven job never completing (River discards it, the
	// same shape TestDailyMetricsRunStaysRunningWhenFinalizeJobIsDiscarded
	// ByRiverUntilRedriven uses): mark the outbox row dead, never call
	// ClaimFinalize/CompleteFinalize for it. The run is now indistinguishable,
	// AT THE daily_metrics_runs LEVEL, from an ordinary CHAOS-4389 discard.
	if _, err := pool.Exec(ctx, `UPDATE worker_job_outbox SET status = 'dead' WHERE dedupe_key = $1`,
		"metrics.daily_finalize:redrive:"+runID+":range-nonce-fail"); err != nil {
		t.Fatal(err)
	}

	// Approval condition (2)/(3): FindStrandedFinalizeRuns must NOT report
	// this run -- it is finalize-redrive's own concern now, not an
	// unattended sweep's.
	strandedRunIDs, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	for _, id := range strandedRunIDs {
		if id == runID {
			t.Fatalf("FindStrandedFinalizeRuns reported a run finalize-redrive already touched: %v", strandedRunIDs)
		}
	}

	// And therefore --all-complete's own bulk redrive (which only ever acts
	// on FindStrandedFinalizeRuns's own candidate list) cannot double-dispatch
	// it either.
	bulkOutcome, err := store.RedriveStrandedFinalize(ctx, publisher, strandedRunIDs, "bulk-nonce", false)
	if err != nil {
		t.Fatalf("RedriveStrandedFinalize: %v", err)
	}
	for _, id := range bulkOutcome.FinalizedRunIDs {
		if id == runID {
			t.Fatalf("RedriveStrandedFinalize (bulk) double-dispatched a run finalize-redrive already touched: %v", bulkOutcome)
		}
	}

	// daily-redrive's own partition-level query is unaffected by
	// construction: the reset never touched daily_metrics_partitions, so it
	// stays 100% 'succeeded' and never matches DispatchablePartitions's own
	// pending/failed eligibility either.
	dispatchable, err := store.DispatchablePartitions(ctx, runID)
	if err != nil {
		t.Fatalf("DispatchablePartitions: %v", err)
	}
	if len(dispatchable) != 0 {
		t.Fatalf("DispatchablePartitions for a finalize-redrive-touched run = %v, want none (partitions were never touched)", dispatchable)
	}
}

// TestFindStrandedFinalizeRunsReincludesARunAfterItsRedrivenFinalizeFails is
// team-lead's escalation on conditions (2)/(3) (2026-08-28), red-first
// against the ORIGINAL (permanent) exclusion design: "a redriven finalize
// that fails leaves the run running-with-failed-finalize and no tool will
// ever recover it" is a silent-failure shape. This exercises the shape that
// finding named specifically -- the redriven job IS claimed (ClaimFinalize
// succeeds, finalization_status='running') and then genuinely fails
// (ReleaseFinalize, finalization_status='failed') -- and proves the run
// becomes visible to an ordinary CHAOS-4389 sweep again immediately, exactly
// like any other stranded run, the moment that failure commits.
func TestFindStrandedFinalizeRunsReincludesARunAfterItsRedrivenFinalizeFails(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)
	var redriveObservations []struct {
		outcome string
		count   int
	}
	store.SetFinalizeRedriveObserver(recordingFinalizeRedriveObserver{sink: &redriveObservations})

	const (
		orgID       = "00000000-0000-4000-8000-000000002501"
		runID       = "00000000-0000-4000-8000-000000002502"
		partitionID = "00000000-0000-4000-8000-000000002503"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-claimed-fail", true, testFinalizeRedriveReason, false)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("RedriveFinalizeForRange outcome = %#v, want redriven_reset_from_succeeded", outcome)
	}

	// RED baseline shape: claim it (as the redriven River job's
	// FinalizeHandler.Work would), then genuinely fail.
	processFinalizeJobAndFail(t, ctx, store, runID)
	var status, finalizationStatus string
	if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&status, &finalizationStatus); err != nil {
		t.Fatal(err)
	}
	if status != "running" || finalizationStatus != "failed" {
		t.Fatalf("run after a failed redriven finalize = status=%q finalization_status=%q, want running/failed", status, finalizationStatus)
	}

	// GREEN: the redrive event closed 'closed_failed' the instant that
	// failure committed -- same transaction, so it can never be out of sync
	// with the run's own state.
	var eventStatus string
	var closedAt *time.Time
	if err := pool.QueryRow(ctx, `
SELECT status, closed_at FROM daily_metrics_finalize_redrive_events
WHERE run_id = $1::uuid ORDER BY created_at LIMIT 1`, runID).
		Scan(&eventStatus, &closedAt); err != nil {
		t.Fatal(err)
	}
	if eventStatus != "closed_failed" || closedAt == nil {
		t.Fatalf("redrive event after a failed redriven finalize = status=%q closed_at=%v, want closed_failed with a timestamp", eventStatus, closedAt)
	}

	// GREEN: FindStrandedFinalizeRuns reports this run again -- exactly like
	// any other ordinary CHAOS-4389 discard, no different treatment.
	strandedRunIDs, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	found := false
	for _, id := range strandedRunIDs {
		if id == runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want it to include %s after its redriven finalize failed", strandedRunIDs, runID)
	}

	// GREEN: an ordinary --run redrive can now recover it too (allowPriorAttempt=true).
	recoveryOutcome, err := store.RedriveStrandedFinalize(ctx, publisher, []string{runID}, "recovery-nonce", true)
	if err != nil {
		t.Fatalf("RedriveStrandedFinalize: %v", err)
	}
	if len(recoveryOutcome.FinalizedRunIDs) != 1 || recoveryOutcome.FinalizedRunIDs[0] != runID {
		t.Fatalf("RedriveStrandedFinalize after a failed redriven finalize = %#v, want %s recovered", recoveryOutcome, runID)
	}

	// Telemetry (team-lead's "count it in telemetry redriven_failed"):
	// exactly one redriven_failed observation, for this run's own failure.
	var redrivenFailedCount int
	for _, observation := range redriveObservations {
		if observation.outcome == "redriven_failed" {
			redrivenFailedCount += observation.count
		}
	}
	if redrivenFailedCount != 1 {
		t.Fatalf("redriven_failed telemetry observations = %d (all: %#v), want 1", redrivenFailedCount, redriveObservations)
	}
}

// TestCompleteFinalizeDoesNotObserveRedrivenFailedTelemetry proves the
// telemetry side of the SUCCESS path stays silent: CompleteFinalize closes
// an open redrive event 'closed_succeeded', never 'redriven_failed' -- the
// two completion paths must not be conflated.
func TestCompleteFinalizeDoesNotObserveRedrivenFailedTelemetry(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)
	var redriveObservations []struct {
		outcome string
		count   int
	}
	store.SetFinalizeRedriveObserver(recordingFinalizeRedriveObserver{sink: &redriveObservations})

	const (
		orgID       = "00000000-0000-4000-8000-000000002601"
		runID       = "00000000-0000-4000-8000-000000002602"
		partitionID = "00000000-0000-4000-8000-000000002603"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	if _, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "range-nonce-success", true, testFinalizeRedriveReason, false); err != nil {
		t.Fatalf("RedriveFinalizeForRange: %v", err)
	}
	processFinalizeJob(t, ctx, store, runID)

	for _, observation := range redriveObservations {
		if observation.outcome == "redriven_failed" {
			t.Fatalf("a successful redriven finalize observed redriven_failed telemetry: %#v", redriveObservations)
		}
	}
}

type recordingFinalizeRedriveObserver struct {
	sink *[]struct {
		outcome string
		count   int
	}
}

func (observer recordingFinalizeRedriveObserver) ObserveDailyMetricsFinalizeRedrive(outcome string, count int) error {
	*observer.sink = append(*observer.sink, struct {
		outcome string
		count   int
	}{outcome: outcome, count: count})
	return nil
}

// TestRedriveFinalizeForRangeDryRunWritesNothing is team-lead's approval
// condition (4): a --dry-run pass reports exactly what a real pass would do
// (which day, which run, whether a terminal-state reset would be needed)
// without writing anything at all -- no reset, no provenance row, no
// outbox row, and (per condition (5)'s telemetry note) no telemetry either.
func TestRedriveFinalizeForRangeDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000002401"
		runID       = "00000000-0000-4000-8000-000000002402"
		partitionID = "00000000-0000-4000-8000-000000002403"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	// dryRun=true: nonce and reason may both be empty (RedriveFinalizeForRange
	// skips the emptiness checks that gate a real invocation).
	outcome, err := store.RedriveFinalizeForRange(ctx, publisher, orgID, targetDay, targetDay, "", true, "", true)
	if err != nil {
		t.Fatalf("RedriveFinalizeForRange(dryRun): %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "would_redrive_reset_from_succeeded" ||
		outcome.Days[0].RunID != runID || !outcome.Days[0].ResetFromSucceeded {
		t.Fatalf("dry-run outcome = %#v, want one would_redrive_reset_from_succeeded day for %s", outcome, runID)
	}

	// Nothing durable changed.
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
	var finalizationStatus string
	if err := pool.QueryRow(ctx, `SELECT finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&finalizationStatus); err != nil {
		t.Fatal(err)
	}
	if finalizationStatus != "succeeded" {
		t.Fatalf("finalization_status after dry-run = %q, want succeeded (untouched)", finalizationStatus)
	}
	var outboxCount, provenanceCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key LIKE $1`,
		"metrics.daily_finalize:redrive:"+runID+"%").Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 0 {
		t.Fatalf("outbox rows after dry-run = %d, want 0", outboxCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_finalize_redrive_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceCount); err != nil {
		t.Fatal(err)
	}
	if provenanceCount != 0 {
		t.Fatalf("provenance rows after dry-run = %d, want 0", provenanceCount)
	}

	// A dry run must not have blinded FindStrandedFinalizeRuns to this run
	// either -- only a REAL redrive (which writes a provenance row) does
	// that.
	strandedRunIDs, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	for _, id := range strandedRunIDs {
		if id == runID {
			t.Fatalf("dry-run's target run unexpectedly appears in FindStrandedFinalizeRuns (it is status='succeeded', never stranded)")
		}
	}
}
