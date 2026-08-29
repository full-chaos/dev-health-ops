//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

// CHAOS-4459: a daily_metrics_run whose partitions are ALL succeeded is
// never reachable through any existing redrive path -- RedriveStrandedPartitions
// (CHAOS-4358) only ever touches a run still status='running', and
// daily-redrive's own eligibility scan excludes 'succeeded' runs by
// construction. This is the exact prod shape CHAOS-4459 names: commit_metrics
// rows written org_id="" by the pre-#1960 writer (CHAOS-4341), under a
// partition the ledger reports 'succeeded'. The tests below prove the reset
// round-trip (a 'succeeded' run becomes claimable again, with a fresh
// generation so the compatibility-bridge execution-ledger identity is new),
// its durable provenance row, the --dry-run preview, and that an
// unsupported family / a run still genuinely in flight are both refused.

const testPartitionRecomputeReason = "CHAOS-4459 test: commit_metrics org-scoped rows are 0 for this day under the pre-#1960 writer"

func TestRedrivePartitionsForRangeResetsAndRedrivesAnAlreadySucceededDay(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000003001"
		runID       = "00000000-0000-4000-8000-000000003002"
		partitionID = "00000000-0000-4000-8000-000000003003"
	)
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
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

	outcome, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "part-nonce-1", "repo_user_commit", testPartitionRecomputeReason, false)
	if err != nil {
		t.Fatalf("RedrivePartitionsForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven" || outcome.Days[0].RunID != runID {
		t.Fatalf("RedrivePartitionsForRange outcome = %#v, want one redriven day for %s", outcome, runID)
	}

	// The reset must be durable and visible before the redriven partition
	// job is processed: this is what makes the run reachable via
	// ClaimPartition again at all (ClaimPartition hard-requires
	// run.status='running').
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")
	var partitionStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID).
		Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "pending" {
		t.Fatalf("partition status after reset = %q, want pending", partitionStatus)
	}

	// A durable provenance row exists, recording the run's exact prior
	// state, written BEFORE the reset.
	var family, priorStatus, priorGeneration, actor, reason, nonce string
	if err := pool.QueryRow(ctx, `
SELECT family, prior_status, prior_generation, actor, reason, nonce
FROM daily_metrics_partition_recompute_events WHERE run_id = $1::uuid`, runID).
		Scan(&family, &priorStatus, &priorGeneration, &actor, &reason, &nonce); err != nil {
		t.Fatalf("provenance row missing: %v", err)
	}
	if family != "repo_user_commit" {
		t.Fatalf("provenance family = %q, want repo_user_commit", family)
	}
	if priorStatus != "succeeded" {
		t.Fatalf("provenance prior_status = %q, want succeeded", priorStatus)
	}
	if priorGeneration != originalGeneration {
		t.Fatalf("provenance prior_generation = %q, want %q", priorGeneration, originalGeneration)
	}
	if actor != "partition-recompute" {
		t.Fatalf("provenance actor = %q, want partition-recompute", actor)
	}
	if reason != testPartitionRecomputeReason {
		t.Fatalf("provenance reason = %q, want %q", reason, testPartitionRecomputeReason)
	}
	if nonce != "part-nonce-1" {
		t.Fatalf("provenance nonce = %q, want part-nonce-1", nonce)
	}

	// CHAOS-4405 precedent applied here: the reset must give the run a
	// FRESH generation, never the original one -- an unchanged generation
	// would make a redriven attempt land on the SAME compatibility-bridge
	// execution-ledger identity and get silently skipped.
	var generationAfterReset string
	if err := pool.QueryRow(ctx, `SELECT generation FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&generationAfterReset); err != nil {
		t.Fatal(err)
	}
	if generationAfterReset == originalGeneration {
		t.Fatalf("generation after reset = %q, want a value different from the original %q", generationAfterReset, originalGeneration)
	}

	var redriveOutboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_partition' AND dedupe_key = $1`,
		"metrics.daily_partition:redrive:"+partitionID+":part-nonce-1").Scan(&redriveOutboxCount); err != nil {
		t.Fatal(err)
	}
	if redriveOutboxCount != 1 {
		t.Fatalf("redrive partition outbox rows = %d, want 1", redriveOutboxCount)
	}

	// Simulate the redriven job's execution reaching CompletePartition --
	// the partition (and, since it is the run's only one, the run's
	// eligibility for finalize) lands back on 'succeeded'.
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil {
		t.Fatalf("ClaimPartition: %v", err)
	}
	if claim == nil {
		t.Fatalf("ClaimPartition: partition %s had nothing claimable after reset", partitionID)
	}
	if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
		t.Fatalf("CompletePartition: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID).
		Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "succeeded" {
		t.Fatalf("partition status after CompletePartition = %q, want succeeded", partitionStatus)
	}
	// CompletePartition only completes the partition and publishes the
	// finalize job -- daily_metrics_runs.status reaches 'succeeded' only
	// once finalize itself completes (CompleteFinalize). Simulate that here
	// so the run is genuinely eligible again for a second recompute pass,
	// exactly like the real system's lifecycle.
	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")

	// Safe to run again for the SAME day: a fresh nonce redrives it again
	// rather than erroring, and appends a SECOND provenance row
	// (append-only, one per invocation) -- the finalize above already
	// re-succeeded the run, so this call's own candidate scan finds it
	// eligible again.
	repeatOutcome, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "part-nonce-2", "repo_user_commit", testPartitionRecomputeReason, false)
	if err != nil {
		t.Fatalf("repeat RedrivePartitionsForRange: %v", err)
	}
	if len(repeatOutcome.Days) != 1 || repeatOutcome.Days[0].Outcome != "redriven" {
		t.Fatalf("repeat RedrivePartitionsForRange outcome = %#v, want redriven again", repeatOutcome)
	}
	var provenanceRowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partition_recompute_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceRowCount); err != nil {
		t.Fatal(err)
	}
	if provenanceRowCount != 2 {
		t.Fatalf("provenance rows after 2 invocations = %d, want 2 (append-only)", provenanceRowCount)
	}
}

// TestRedrivePartitionsForRangeReportsDaysWithNoEligibleRun proves a day in
// the requested range with no run at all is reported skipped_ineligible,
// never silently dropped from the outcome.
func TestRedrivePartitionsForRangeReportsDaysWithNoEligibleRun(t *testing.T) {
	ctx := context.Background()
	_, store, publisher := newFinalizeRedriveTestStack(t)

	const orgID = "00000000-0000-4000-8000-000000003101"
	targetDay := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)

	outcome, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "part-nonce-3", "repo_user_commit", testPartitionRecomputeReason, false)
	if err != nil {
		t.Fatalf("RedrivePartitionsForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "skipped_ineligible" || outcome.Days[0].RunID != "" {
		t.Fatalf("outcome = %#v, want one skipped_ineligible day with no run id", outcome)
	}
}

// TestRedrivePartitionsForRangeSkipsARunningRun proves this verb refuses a
// run that is still genuinely in flight (status='running') -- that shape is
// daily-redrive's/the automatic path's territory, not this one's: resetting
// it here would race whatever is currently executing it.
func TestRedrivePartitionsForRangeSkipsARunningRun(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000003201"
		runID       = "00000000-0000-4000-8000-000000003202"
		partitionID = "00000000-0000-4000-8000-000000003203"
	)
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "pending", now)
	// insertFinalizeTestRun's own default is status='running' -- left as-is,
	// simulating a run mid-dispatch with a not-yet-succeeded partition.

	outcome, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "part-nonce-4", "repo_user_commit", testPartitionRecomputeReason, false)
	if err != nil {
		t.Fatalf("RedrivePartitionsForRange: %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "skipped_ineligible" {
		t.Fatalf("outcome = %#v, want skipped_ineligible for a still-running run", outcome)
	}
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")
	var provenanceRowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partition_recompute_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceRowCount); err != nil {
		t.Fatal(err)
	}
	if provenanceRowCount != 0 {
		t.Fatalf("provenance rows for a run this verb correctly refused = %d, want 0", provenanceRowCount)
	}
}

// TestRedrivePartitionsForRangeRejectsUnsupportedFamily proves --family is a
// closed vocabulary, not free text.
func TestRedrivePartitionsForRangeRejectsUnsupportedFamily(t *testing.T) {
	ctx := context.Background()
	_, store, publisher := newFinalizeRedriveTestStack(t)

	const orgID = "00000000-0000-4000-8000-000000003301"
	targetDay := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)

	if _, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "part-nonce-5", "dora", testPartitionRecomputeReason, false); err != ErrInvalidState {
		t.Fatalf("RedrivePartitionsForRange with unsupported family: err = %v, want ErrInvalidState", err)
	}
}

// TestRedrivePartitionsForRangeDryRunWritesNothing proves --dry-run is
// provably free of durable side effects: every transaction it opens is
// rolled back, never committed.
func TestRedrivePartitionsForRangeDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000003401"
		runID       = "00000000-0000-4000-8000-000000003402"
		partitionID = "00000000-0000-4000-8000-000000003403"
	)
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, runID); err != nil {
		t.Fatal(err)
	}

	// dryRun=true: nonce and reason may both be empty.
	outcome, err := store.RedrivePartitionsForRange(ctx, publisher, orgID, targetDay, targetDay, "", "repo_user_commit", "", true)
	if err != nil {
		t.Fatalf("RedrivePartitionsForRange(dryRun): %v", err)
	}
	if len(outcome.Days) != 1 || outcome.Days[0].Outcome != "would_redrive" || outcome.Days[0].RunID != runID {
		t.Fatalf("dry-run outcome = %#v, want one would_redrive day for %s", outcome, runID)
	}

	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
	var partitionStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID).
		Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "succeeded" {
		t.Fatalf("partition status after dry-run = %q, want succeeded (untouched)", partitionStatus)
	}
	var outboxCount, provenanceCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE dedupe_key LIKE $1`,
		"metrics.daily_partition:redrive:"+partitionID+"%").Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 0 {
		t.Fatalf("outbox rows after dry-run = %d, want 0", outboxCount)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partition_recompute_events WHERE run_id = $1::uuid`, runID).
		Scan(&provenanceCount); err != nil {
		t.Fatal(err)
	}
	if provenanceCount != 0 {
		t.Fatalf("provenance rows after dry-run = %d, want 0", provenanceCount)
	}
}
