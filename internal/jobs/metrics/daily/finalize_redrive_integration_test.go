//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4389: 12 prod daily_metrics_runs rows sat status='running' with 100%
// of their partitions 'succeeded' -- the finalize step was never
// dispatched/applied once the last partition succeeded. The three tests below
// exercise every path the ticket named for a run reaching a terminal state
// once every partition succeeds: a redriven/reclaimed last partition, two
// partitions completing concurrently, and the last partition succeeding
// through the ordinary claim path while its one metrics.daily_finalize
// dispatch is discarded by River. Only the third is the actual gap: the
// first two already reach 'succeeded' on origin/main. Checked out against
// origin/main with FindStrandedFinalizeRuns/RedriveStrandedFinalize/
// PublishRedriveFinalizeTx removed, TestDailyMetricsRunStaysRunningWhen
// FinalizeJobIsDiscardedByRiverUntilRedriven does not compile -- the gap is
// that no existing Store/Publisher capability can move a run out of this
// state at all, not merely that some existing call returns the wrong thing.

func newFinalizeRedriveTestStack(t *testing.T) (*pgxpool.Pool, *PostgresStore, *PostgresPublisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return pool, store, publisher
}

func insertFinalizeTestRun(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	runID, orgID string, targetDay, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,$3,'post-sync:finalize-gap','running','pending',$4,$4)`,
		runID, orgID, targetDay, now); err != nil {
		t.Fatal(err)
	}
}

func insertFinalizeTestPartition(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	partitionID, runID string, ordinal int, status string, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at)
VALUES ($1,$2,$3,'[]'::jsonb,$4,1,$5,$5)`,
		partitionID, runID, ordinal, status, now); err != nil {
		t.Fatal(err)
	}
}

// processFinalizeJob simulates one metrics.daily_finalize River execution
// reaching FinalizeHandler.Work and succeeding: claim, then complete. It
// stands in for the compatibility-bridge Finalize() call succeeding, which
// is out of scope for this Store-level test.
func processFinalizeJob(t *testing.T, ctx context.Context, store *PostgresStore, runID string) {
	t.Helper()
	claim, err := store.ClaimFinalize(ctx, runID)
	if err != nil {
		t.Fatalf("ClaimFinalize: %v", err)
	}
	if claim == nil {
		t.Fatalf("ClaimFinalize: run %s had nothing claimable", runID)
	}
	if err := store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatalf("CompleteFinalize: %v", err)
	}
}

func assertFinalizeTestRunStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, want string) {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != want {
		t.Fatalf("run %s status = %q, want %q", runID, status, want)
	}
}

// TestDailyMetricsRunReachesTerminalStateWhenLastPartitionCompletesViaRedrive
// covers path 1 from CHAOS-4389: the LAST remaining partition is a
// previously-'failed' one, reclaimed and completed exactly as a redriven
// metrics.daily_partition job would (ClaimPartition already treats a
// 'failed' partition as claimable -- the CHAOS-4358 redrive machinery exists
// to get a job's execution BACK to this handler, not to change what the
// handler does once it arrives). This path already works on origin/main.
func TestDailyMetricsRunReachesTerminalStateWhenLastPartitionCompletesViaRedrive(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID              = "00000000-0000-4000-8000-000000000701"
		runID              = "00000000-0000-4000-8000-000000000702"
		succeededPartition = "00000000-0000-4000-8000-000000000703"
		redrivenPartition  = "00000000-0000-4000-8000-000000000704"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, succeededPartition, runID, 0, "succeeded", now)
	insertFinalizeTestPartition(t, ctx, pool, redrivenPartition, runID, 1, "failed", now)

	claim, err := store.ClaimPartition(ctx, redrivenPartition)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition (redrive reclaim) = %#v, %v", claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
		t.Fatalf("CompletePartition: %v", err)
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key = $1`,
		"metrics.daily_finalize:"+runID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("finalize outbox rows = %d, want 1", outboxCount)
	}

	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
}

// TestDailyMetricsRunReachesTerminalStateWhenLastTwoPartitionsCompleteConcurrently
// covers path 2 from CHAOS-4389: two workers complete the run's last two
// partitions at (as close to) the same instant. CompletePartition's
// `SELECT ... FOR UPDATE` on the run row must serialize the two
// transactions, so exactly one of them observes incomplete=0 and publishes
// finalize -- never zero (the run would strand) and never two (a duplicate
// finalize attempt). This path already works on origin/main.
func TestDailyMetricsRunReachesTerminalStateWhenLastTwoPartitionsCompleteConcurrently(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID      = "00000000-0000-4000-8000-000000000801"
		runID      = "00000000-0000-4000-8000-000000000802"
		partitionA = "00000000-0000-4000-8000-000000000803"
		partitionB = "00000000-0000-4000-8000-000000000804"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionA, runID, 0, "pending", now)
	insertFinalizeTestPartition(t, ctx, pool, partitionB, runID, 1, "pending", now)

	claimA, err := store.ClaimPartition(ctx, partitionA)
	if err != nil || claimA == nil {
		t.Fatalf("ClaimPartition A = %#v, %v", claimA, err)
	}
	claimB, err := store.ClaimPartition(ctx, partitionB)
	if err != nil || claimB == nil {
		t.Fatalf("ClaimPartition B = %#v, %v", claimB, err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for _, claim := range []PartitionClaim{*claimA, *claimB} {
		claim := claim
		go func() {
			defer wg.Done()
			<-start
			errs <- store.CompletePartition(ctx, claim, publisher)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent CompletePartition: %v", err)
		}
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key = $1`,
		"metrics.daily_finalize:"+runID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("finalize outbox rows after concurrent completion = %d, want exactly 1 (not 0, not 2)", outboxCount)
	}

	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
}

// TestDailyMetricsRunStaysRunningWhenFinalizeJobIsDiscardedByRiverUntilRedriven
// is the CHAOS-4389 red-first proof, path 3: the run's last partition
// succeeds through the ordinary claim path (nothing unusual happens here),
// CompletePartition enqueues the ONE metrics.daily_finalize job it ever will
// under the fixed idempotency key "metrics.daily_finalize:"+run.ID -- and
// that job is discarded by River (simulated here by marking the outbox row
// 'dead', the terminal status a live relay/reconciler would leave it in)
// before FinalizeHandler.Work ever calls CompleteFinalize. Because the
// outbox permanently remembers that dedupe key ("ON CONFLICT (dedupe_key) DO
// NOTHING"), nothing on origin/main ever re-enqueues it: the run is stranded
// status='running' forever with 100% of its partitions succeeded -- exactly
// the 12-row prod shape. FindStrandedFinalizeRuns/RedriveStrandedFinalize
// close the gap by publishing a fresh, nonce-scoped finalize job.
func TestDailyMetricsRunStaysRunningWhenFinalizeJobIsDiscardedByRiverUntilRedriven(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000000901"
		runID       = "00000000-0000-4000-8000-000000000902"
		partitionID = "00000000-0000-4000-8000-000000000903"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "pending", now)

	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition = %#v, %v", claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
		t.Fatalf("CompletePartition: %v", err)
	}

	// Precondition: the ordinary finalize job landed under the fixed key.
	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key = $1`,
		"metrics.daily_finalize:"+runID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("finalize outbox rows before discard = %d, want 1", outboxCount)
	}

	// Simulate River discarding it: no execution of FinalizeHandler.Work ever
	// happens for this run, so ClaimFinalize is never called and
	// finalization_status stays 'pending' forever.
	if _, err := pool.Exec(ctx, `UPDATE worker_job_outbox SET status = 'dead' WHERE dedupe_key = $1`, "metrics.daily_finalize:"+runID); err != nil {
		t.Fatal(err)
	}

	// The gap: the run is durably stuck. Advancing the clock far past any
	// lease does nothing, because nothing is ever claimed in the first place.
	now = now.Add(24 * time.Hour)
	store.now = func() time.Time { return now }
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")

	strandedRunIDs, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	if len(strandedRunIDs) != 1 || strandedRunIDs[0] != runID {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want [%s]", strandedRunIDs, runID)
	}

	outcome, err := store.RedriveStrandedFinalize(ctx, publisher, strandedRunIDs, "finalize-redrive-nonce-1", false)
	if err != nil {
		t.Fatalf("RedriveStrandedFinalize: %v", err)
	}
	if len(outcome.FinalizedRunIDs) != 1 || outcome.FinalizedRunIDs[0] != runID {
		t.Fatalf("RedriveStrandedFinalize outcome = %#v, want FinalizedRunIDs=[%s]", outcome, runID)
	}

	var redriveOutboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key = $1`,
		"metrics.daily_finalize:redrive:"+runID+":finalize-redrive-nonce-1").Scan(&redriveOutboxCount); err != nil {
		t.Fatal(err)
	}
	if redriveOutboxCount != 1 {
		t.Fatalf("redrive finalize outbox rows = %d, want 1 (fresh dedupe key, not the dead original)", redriveOutboxCount)
	}

	// A repeated sweep before the redriven job is processed must not find or
	// re-redrive this run again: FinalizeHandler.Work has not run yet, but
	// FindStrandedFinalizeRuns only inspects run/partition state, which has
	// not changed -- so it MUST still report it as detected until an
	// operator actually processes the fresh job. This documents current
	// behavior (repeated detection is expected and safe; RedriveStrandedFinalize's
	// own dedupe key makes a repeated redrive idempotent, not the detection
	// query) rather than asserting a stronger guarantee this function does
	// not make.
	strandedAgain, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("second FindStrandedFinalizeRuns: %v", err)
	}
	if len(strandedAgain) != 1 || strandedAgain[0] != runID {
		t.Fatalf("second FindStrandedFinalizeRuns = %v, want [%s] (still detected until processed)", strandedAgain, runID)
	}

	// Now simulate the redriven job's execution reaching FinalizeHandler.Work.
	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")

	// Safe to run again: nothing left eligible.
	finalSweep, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("third FindStrandedFinalizeRuns: %v", err)
	}
	if len(finalSweep) != 0 {
		t.Fatalf("FindStrandedFinalizeRuns after completion = %v, want none", finalSweep)
	}
	repeatOutcome, err := store.RedriveStrandedFinalize(ctx, publisher, []string{runID}, "finalize-redrive-nonce-2", false)
	if err != nil {
		t.Fatalf("repeat RedriveStrandedFinalize: %v", err)
	}
	if len(repeatOutcome.FinalizedRunIDs) != 0 {
		t.Fatalf("repeat RedriveStrandedFinalize on a completed run = %#v, want no-op", repeatOutcome)
	}
}

// TestFindStrandedFinalizeRunsExcludesRunWithZeroPartitions is the codex
// review red-first proof (P1, round 1): a run between ClaimDispatch (which
// sets status='running') and MaterializeScheduledFanout (which inserts the
// first partition rows) has ZERO partitions. The "every partition succeeded"
// check is a bare NOT EXISTS(status <> 'succeeded'), which is vacuously true
// when there are no partition rows at all -- without an explicit EXISTS
// check alongside it, this function would treat "hasn't started" identically
// to "100% succeeded" and finalize a run that has computed nothing.
func TestFindStrandedFinalizeRunsExcludesRunWithZeroPartitions(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID = "00000000-0000-4000-8000-000000001001"
		runID = "00000000-0000-4000-8000-000000001002"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	// Deliberately NO partition rows inserted: this is the window between
	// ClaimDispatch and MaterializeScheduledFanout, not a stranded finalize.

	strandedRunIDs, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	if len(strandedRunIDs) != 0 {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want none (run has zero partitions, not 100%% succeeded)", strandedRunIDs)
	}

	// Even an operator explicitly naming this run (as --run would) must not
	// finalize it: the transactional recheck has the identical obligation.
	outcome, err := store.RedriveStrandedFinalize(ctx, publisher, []string{runID}, "zero-partition-nonce", true)
	if err != nil {
		t.Fatalf("RedriveStrandedFinalize: %v", err)
	}
	if len(outcome.FinalizedRunIDs) != 0 {
		t.Fatalf("RedriveStrandedFinalize on a zero-partition run = %#v, want no-op", outcome)
	}
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")
}

// TestRedriveStrandedFinalizeRequiresExplicitRunForPriorAttempt is the codex
// review red-first proof (P1, round 1): finalization_status='failed' does
// NOT mean finalize never ran -- FinalizeHandler.Work sets it both when the
// compatibility call itself failed AND when it SUCCEEDED (writing real,
// durable output) but the subsequent CompleteFinalize bookkeeping write
// failed. A bulk sweep (allowPriorAttempt=false, what --all-complete passes)
// must never blindly redrive that shape -- only an operator naming one
// specific run (allowPriorAttempt=true, what --run passes) after actually
// checking it may.
func TestRedriveStrandedFinalizeRequiresExplicitRunForPriorAttempt(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000001101"
		runID       = "00000000-0000-4000-8000-000000001102"
		partitionID = "00000000-0000-4000-8000-000000001103"
	)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	targetDay := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, targetDay, now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	// Simulate a finalize attempt that reached Finalize() (and, in the real
	// system, may have already written durable output) before its
	// CompleteFinalize bookkeeping write failed: finalization_status='failed'
	// with everything else already terminal.
	if _, err := pool.Exec(ctx, `UPDATE daily_metrics_runs SET finalization_status = 'failed' WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}

	// Detection still surfaces it -- an operator needs to know about it even
	// though the bulk path below will not touch it.
	detected, err := store.FindStrandedFinalizeRuns(ctx, 0)
	if err != nil {
		t.Fatalf("FindStrandedFinalizeRuns: %v", err)
	}
	if len(detected) != 1 || detected[0] != runID {
		t.Fatalf("FindStrandedFinalizeRuns = %v, want [%s]", detected, runID)
	}

	// The bulk/--all-complete path (allowPriorAttempt=false) must skip it.
	bulkOutcome, err := store.RedriveStrandedFinalize(ctx, publisher, detected, "bulk-nonce", false)
	if err != nil {
		t.Fatalf("bulk RedriveStrandedFinalize: %v", err)
	}
	if len(bulkOutcome.FinalizedRunIDs) != 0 {
		t.Fatalf("bulk RedriveStrandedFinalize (allowPriorAttempt=false) = %#v, want no-op for a 'failed' finalization_status", bulkOutcome)
	}
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "running")

	// The explicit --run path (allowPriorAttempt=true) may.
	explicitOutcome, err := store.RedriveStrandedFinalize(ctx, publisher, []string{runID}, "explicit-nonce", true)
	if err != nil {
		t.Fatalf("explicit RedriveStrandedFinalize: %v", err)
	}
	if len(explicitOutcome.FinalizedRunIDs) != 1 || explicitOutcome.FinalizedRunIDs[0] != runID {
		t.Fatalf("explicit RedriveStrandedFinalize (allowPriorAttempt=true) = %#v, want FinalizedRunIDs=[%s]", explicitOutcome, runID)
	}

	processFinalizeJob(t, ctx, store, runID)
	assertFinalizeTestRunStatus(t, ctx, pool, runID, "succeeded")
}
