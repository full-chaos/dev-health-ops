//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testFeatureDisabledMessage stands in for a real
// CanonicalIncidentDecisionForUpdate reason in tests that exercise
// terminalizeFeatureDisabledRun/Graph directly (below terminalizeFeatureDisabledPlan,
// which is what actually resolves a real reason) -- the specific reason
// value is immaterial to these tests, which assert on the run/unit
// termination mechanics, not on message content.
var testFeatureDisabledMessage = canonicalIncidentFeatureDisabledMessage(scheduledsync.FeatureDecisionReasonGlobalDisabled)

// createFeatureDisabledTables is a self-contained schema for this file:
// createFinalizeTables' sync_runs/sync_run_units carry the full column set
// terminalizeFeatureDisabledRun/Graph need (status, total_units, result,
// etc.), which the reference-discovery test file's leaner schema does not,
// so this is its own copy rather than a reuse of either.
func createFeatureDisabledTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE sync_dispatch_outbox (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL, kind text NOT NULL,
 status text NOT NULL, available_at timestamptz NOT NULL, attempts int NOT NULL DEFAULT 0,
 dispatched_at timestamptz NULL, dispatched_transport text NULL, dispatched_route_generation bigint NULL,
 transport_job_id text NULL, claim_token text NULL, claim_transport text NULL,
 claim_route_generation bigint NULL, claim_expires_at timestamptz NULL, last_error text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE (sync_run_id, kind)
);
CREATE TABLE sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NULL,
 status text NOT NULL, total_units int NOT NULL DEFAULT 0, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, completed_at timestamptz NULL, result json NULL, error text NULL
);
CREATE TABLE sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, source_id uuid NULL, status text NOT NULL,
 cost_class text NOT NULL DEFAULT 'standard',
 available_at timestamptz NULL, lease_owner text NULL, lease_expires_at timestamptz NULL,
 error text NULL, result json NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sync_run_reference_discoveries (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
 status text NOT NULL, attempts int NOT NULL DEFAULT 0, available_at timestamptz NOT NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 completed_at timestamptz NULL, error text NULL, result json NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE integrations (
 id uuid PRIMARY KEY, provider text NOT NULL
);
CREATE TABLE integration_datasets (
 id uuid PRIMARY KEY, integration_id uuid NOT NULL, dataset_key text NOT NULL, is_enabled boolean NOT NULL
);
CREATE TABLE backfill_jobs (
 id uuid PRIMARY KEY, org_id text NOT NULL, celery_task_id text NULL, status text NOT NULL,
 total_chunks int NOT NULL DEFAULT 0, completed_chunks int NOT NULL DEFAULT 0,
 failed_chunks int NOT NULL DEFAULT 0, error_message text NULL, completed_at timestamptz NULL
);
CREATE TABLE scheduled_jobs (
 id uuid PRIMARY KEY
);
CREATE TABLE job_runs (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES scheduled_jobs(id),
 status int NOT NULL, completed_at timestamptz NULL, result json NULL, error text NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	featureDisabledTestOrg         = "00000000-0000-4000-8000-0000000000e1"
	featureDisabledTestRun         = "00000000-0000-4000-8000-0000000000e2"
	featureDisabledTestIntegration = "00000000-0000-4000-8000-0000000000e3"
)

func startFeatureDisabledPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
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
	return ctx, pool
}

func withTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func seedFeatureDisabledRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, totalUnits int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id, org_id, integration_id, status, total_units, completed_units, failed_units)
VALUES ($1, $2, $3, 'dispatching', $4, 0, 0)`,
		featureDisabledTestRun, featureDisabledTestOrg, featureDisabledTestIntegration, totalUnits); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO integrations (id, provider) VALUES ($1, 'github')`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
}

func insertFeatureDisabledUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string, leaseOwner *string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, provider, dataset_key, status, lease_owner)
VALUES ($1, $2, $3, 'github', 'prs', $4, $5)`,
		id, featureDisabledTestOrg, featureDisabledTestRun, status, leaseOwner); err != nil {
		t.Fatal(err)
	}
}

// TestTerminalizeFeatureDisabledRunBulkAndRaceSafeRunning pins
// terminalize_feature_disabled_run's two-phase termination: the bulk
// unclaimed-status update, and the per-RUNNING-unit lease-owner-matched
// update -- specifically that a RUNNING unit with a NULL lease_owner is
// still terminalized (IS NOT DISTINCT FROM, not a plain `=` which never
// matches NULL). An already-terminal SUCCESS unit must be left untouched.
func TestTerminalizeFeatureDisabledRunBulkAndRaceSafeRunning(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 5)

	planned := "00000000-0000-4000-8000-0000000001a1"
	retrying := "00000000-0000-4000-8000-0000000001a2"
	dispatching := "00000000-0000-4000-8000-0000000001a3"
	runningNilOwner := "00000000-0000-4000-8000-0000000001a4"
	success := "00000000-0000-4000-8000-0000000001a5"
	insertFeatureDisabledUnit(t, ctx, pool, planned, "planned", nil)
	insertFeatureDisabledUnit(t, ctx, pool, retrying, "retrying", nil)
	insertFeatureDisabledUnit(t, ctx, pool, dispatching, "dispatching", nil)
	insertFeatureDisabledUnit(t, ctx, pool, runningNilOwner, "running", nil)
	insertFeatureDisabledUnit(t, ctx, pool, success, "success", nil)

	var transition FeatureDisabledRunTransition
	now := time.Now().UTC()
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		result, err := terminalizeFeatureDisabledRun(ctx, tx, run, testFeatureDisabledMessage, now)
		if err != nil {
			t.Fatalf("terminalizeFeatureDisabledRun: %v", err)
		}
		transition = result
		if run.status != syncRunStatusFailed {
			t.Fatalf("run.status=%q want=%q", run.status, syncRunStatusFailed)
		}
		if run.completedAt == nil {
			t.Fatal("run.completedAt not set when RunTerminal")
		}
		if run.completedUnits != 1 || run.failedUnits != 4 {
			t.Fatalf("run.completedUnits=%d run.failedUnits=%d want 1,4", run.completedUnits, run.failedUnits)
		}
	})

	if transition.FailedUnits != 4 {
		t.Fatalf("FailedUnits=%d want=4 (planned+retrying+dispatching+running-with-nil-lease)", transition.FailedUnits)
	}
	if transition.RunningUnits != 0 {
		t.Fatalf("RunningUnits=%d want=0", transition.RunningUnits)
	}
	if !transition.RunTerminal {
		t.Fatal("RunTerminal=false want=true (4 failed + 1 success == 5 total)")
	}

	for _, id := range []string{planned, retrying, dispatching, runningNilOwner} {
		var status string
		var errorText *string
		if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, id).Scan(&status, &errorText); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusFailed {
			t.Fatalf("unit %s status=%q want=failed", id, status)
		}
		if errorText == nil || *errorText != testFeatureDisabledMessage {
			t.Fatalf("unit %s error=%v want=%q", id, errorText, testFeatureDisabledMessage)
		}
	}
	var successStatus string
	var successError *string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, success).Scan(&successStatus, &successError); err != nil {
		t.Fatal(err)
	}
	if successStatus != syncRunUnitStatusSuccess || successError != nil {
		t.Fatalf("already-terminal success unit was touched: status=%q error=%v", successStatus, successError)
	}

	var runStatus string
	var completedUnits, failedUnits int
	var runError string
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units, error FROM sync_runs WHERE id=$1`, featureDisabledTestRun).
		Scan(&runStatus, &completedUnits, &failedUnits, &runError); err != nil {
		t.Fatal(err)
	}
	if runStatus != syncRunStatusFailed || completedUnits != 1 || failedUnits != 4 {
		t.Fatalf("sync_runs row status=%q completed=%d failed=%d want failed,1,4", runStatus, completedUnits, failedUnits)
	}
	if runError != testFeatureDisabledMessage {
		t.Fatalf("sync_runs.error=%q want=%q", runError, testFeatureDisabledMessage)
	}
}

// TestTerminalizeFeatureDisabledRunWaitsForAConcurrentRollupWriterThenSeesItsResult
// pins codex round 10's P1 fix (CHAOS-4586): terminalizeFeatureDisabledRun
// now locks the sync_runs row (syncrunrollup.LockRun) before counting unit
// statuses. Without that lock, a concurrent syncrunrollup.Bump-style
// writer on the SAME run that commits between this function's counting
// read and its own later sync_runs write would have its fresh counts
// silently overwritten by this function's now-stale ones -- neither write
// is a compare-and-swap, so whichever commits last wins outright.
//
// This is a genuine concurrency proof, not a source-text scan or a
// same-transaction lock probe (which cannot distinguish "locked before
// counting" from "locked implicitly by this function's own later UPDATE",
// since by the time terminalizeFeatureDisabledRun returns it has already
// written sync_runs regardless of whether the fix is present). It holds a
// producer transaction's lock on the run's sync_runs row, starts
// terminalizeFeatureDisabledRun concurrently in a second transaction,
// confirms via pg_stat_activity (deterministic polling, same technique as
// TestRollbackBlocksOnAnOpenProducerTransactionAndUnblocksOnCommit) that
// it is ACTUALLY BLOCKED waiting for that exact lock -- proving the lock
// call happens before anything else, not just eventually -- then commits
// the producer's own unit completion and asserts the unblocked call's
// count reflects it.
func TestTerminalizeFeatureDisabledRunWaitsForAConcurrentRollupWriterThenSeesItsResult(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	// total_units=2: one PLANNED unit that already exists (the consumer's
	// own phase-1 bulk update fails it) and one the PRODUCER inserts,
	// already 'success', only after confirming the consumer is blocked.
	// The producer's unit does not exist yet when the consumer runs its
	// own phase-1/phase-2 unit writes, so there is no sync_run_units row
	// lock contention between them -- the ONLY resource they contend on
	// is the sync_runs row itself, which is the property this test exists
	// to isolate.
	seedFeatureDisabledRun(t, ctx, pool, 2)
	plannedUnit := "00000000-0000-4000-8000-0000000001c2"
	producerUnit := "00000000-0000-4000-8000-0000000001c1"
	insertFeatureDisabledUnit(t, ctx, pool, plannedUnit, "planned", nil)

	producerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A pgx.Tx that is never committed nor rolled back holds its
	// connection (and any row locks) forever -- including through an
	// early t.Fatal unwinding this goroutine. Without this, a failure
	// anywhere below would leave the consumer goroutine permanently
	// blocked on the still-held lock, wedging the whole test binary.
	defer func() { _ = producerTx.Rollback(ctx) }()
	if err := syncrunrollup.LockRun(ctx, producerTx, featureDisabledTestRun); err != nil {
		t.Fatalf("producer LockRun: %v", err)
	}

	type consumerResult struct {
		transition FeatureDisabledRunTransition
		err        error
	}
	consumerDone := make(chan consumerResult, 1)
	go func() {
		consumerTx, err := pool.Begin(ctx)
		if err != nil {
			consumerDone <- consumerResult{err: err}
			return
		}
		defer func() { _ = consumerTx.Rollback(ctx) }()
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		transition, err := terminalizeFeatureDisabledRun(ctx, consumerTx, run, testFeatureDisabledMessage, time.Now().UTC())
		if err == nil {
			err = consumerTx.Commit(ctx)
		}
		consumerDone <- consumerResult{transition: transition, err: err}
	}()
	waitForBlockedSyncRunsLock(t, ctx, pool, "terminalizeFeatureDisabledRun")

	// The consumer is provably blocked right now (waitForBlockedSyncRunsLock
	// only returns once Postgres itself reports a lock-waiter on this
	// exact row) -- by construction, this means its OWN phase-1/phase-2
	// unit writes (which run before it ever calls LockRun) have already
	// executed, touching only plannedUnit. Only after the producer commits
	// may it proceed.
	select {
	case result := <-consumerDone:
		t.Fatalf("terminalizeFeatureDisabledRun completed before the producer's transaction committed "+
			"(result=%+v) -- it is not locking the run before counting (codex round 10, CHAOS-4586)", result)
	default:
	}

	// The producer's unit is inserted as PART of producerTx (not a
	// separate auto-committing statement) so it becomes visible to the
	// blocked consumer ONLY once producerTx's own commit below succeeds --
	// exactly the ordering this test exists to prove terminalizeFeatureDisabledRun
	// now respects. It never existed before this point, so it was invisible
	// to (and untouched by) the consumer's own earlier phase-1/phase-2 writes.
	if _, err := producerTx.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, provider, dataset_key, status)
VALUES ($1, $2, $3, 'github', 'prs', 'success')`,
		producerUnit, featureDisabledTestOrg, featureDisabledTestRun); err != nil {
		t.Fatalf("producer insert on its own transaction: %v", err)
	}
	if err := producerTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-consumerDone:
		if result.err != nil {
			t.Fatalf("terminalizeFeatureDisabledRun (after producer commit) = %v, want nil", result.err)
		}
		// 1 failed (the planned unit this call itself terminalizes); the
		// producer's unit is success, not failed. If this call's counting
		// query had run BEFORE the producer's lock released, the producer's
		// unit would not exist yet at all, but that would make its OWN
		// write (below) wrong at the sync_runs row, not this in-memory
		// value -- the real assertion is the sync_runs row check after.
		if result.transition.FailedUnits != 1 {
			t.Fatalf("FailedUnits=%d want=1 (only the planned unit)", result.transition.FailedUnits)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("terminalizeFeatureDisabledRun never unblocked after the producer committed")
	}

	var completedUnits, failedUnits int
	if err := pool.QueryRow(ctx, `SELECT completed_units, failed_units FROM sync_runs WHERE id = $1`, featureDisabledTestRun).
		Scan(&completedUnits, &failedUnits); err != nil {
		t.Fatal(err)
	}
	if completedUnits != 1 || failedUnits != 1 {
		t.Fatalf("sync_runs completed_units=%d failed_units=%d, want 1,1 -- the producer's concurrent "+
			"success was invisible to (or overwritten by) a stale count (codex round 10, CHAOS-4586)", completedUnits, failedUnits)
	}
}

// TestTerminalizeFeatureDisabledRunAcquiresTheBucketAdvisoryLockBeforeAnyUnitWrite
// pins codex round 11's P1: terminalizeFeatureDisabledRun's own bulk UPDATE
// (planned/retrying/dispatching, a bare status-IN predicate with no
// explicit row-lock order -- Postgres locks matching rows in whatever
// order its plan finds them) and its per-unit running-lease loop are a
// FIFTH bulk/multi-row writer of sync_run_units, alongside dispatch's
// claimUnits, AuthorizeRun's hard-cap denial writes, LeaseRepair.Step and
// UnreclaimableSweep.Step -- all four of which already take the SAME
// sorted per-(orgID, provider, costClass) advisory lock before touching
// any row, precisely because Postgres gives no row-lock-order guarantee
// across independently evolving code paths that never agree on one row
// order between them. This function never did, until now: round 4's
// UnreclaimableSweep.Step fix (deterministic ascending-unit-id row
// locking) can put this function's own unordered bulk scan in the
// OPPOSITE row order from the sweep's, for the same two units -- a
// genuine ABBA deadlock Postgres detects and aborts one side of.
//
// This is a genuine concurrency proof, not a source-text scan (same class
// as this PR's other two concurrency proofs, both in this file and in
// budget_chokepoint_integration_test.go): a holder transaction takes the
// SAME bucket's advisory lock this function's own fix now takes first, and
// holds it; terminalizeFeatureDisabledRun is started concurrently and MUST
// block on it, confirmed via pg_stat_activity polling (the blocked
// session's own query text is the literal pg_advisory_xact_lock call, the
// same technique waitForBlockedSyncRunsLock above uses for a row lock).
// Once confirmed blocked, the holder releases, and the call proceeds to
// completion normally -- proving both that the lock is acquired at all,
// and that it does not wedge a run that has no actual contender.
func TestTerminalizeFeatureDisabledRunAcquiresTheBucketAdvisoryLockBeforeAnyUnitWrite(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	unitID := "00000000-0000-4000-8000-0000000001c3"
	// insertFeatureDisabledUnit hardcodes provider='github'; cost_class
	// defaults to 'standard' (schema default) -- this unit's bucket is
	// therefore (featureDisabledTestOrg, "github", "standard").
	insertFeatureDisabledUnit(t, ctx, pool, unitID, "planned", nil)
	bucketKey := bucketAdvisoryLockKey(dispatchBucket{
		orgID: featureDisabledTestOrg, provider: "github", costClass: "standard",
	})

	holderTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holderTx.Rollback(ctx) }()
	if _, err := holderTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, bucketKey); err != nil {
		t.Fatalf("holder acquire bucket lock: %v", err)
	}

	type consumerResult struct {
		transition FeatureDisabledRunTransition
		err        error
	}
	consumerDone := make(chan consumerResult, 1)
	go func() {
		consumerTx, err := pool.Begin(ctx)
		if err != nil {
			consumerDone <- consumerResult{err: err}
			return
		}
		defer func() { _ = consumerTx.Rollback(ctx) }()
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		transition, err := terminalizeFeatureDisabledRun(ctx, consumerTx, run, testFeatureDisabledMessage, time.Now().UTC())
		if err == nil {
			err = consumerTx.Commit(ctx)
		}
		consumerDone <- consumerResult{transition: transition, err: err}
	}()

	waitForBlockedAdvisoryLock(t, ctx, pool, "terminalizeFeatureDisabledRun")

	select {
	case result := <-consumerDone:
		t.Fatalf("terminalizeFeatureDisabledRun completed (result=%+v) before the holder released the bucket "+
			"advisory lock -- it is not acquiring the lock at all (codex round 11, CHAOS-4586)", result)
	default:
	}

	if err := holderTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-consumerDone:
		if result.err != nil {
			t.Fatalf("terminalizeFeatureDisabledRun (after the bucket lock released): %v", result.err)
		}
		if !result.transition.RunTerminal {
			t.Fatalf("transition.RunTerminal=false, want true")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("terminalizeFeatureDisabledRun never unblocked after the bucket advisory lock was released")
	}
}

// waitForBlockedAdvisoryLock polls pg_stat_activity for a session blocked
// on ANY pg_advisory_xact_lock call -- the literal query text
// acquireBucketAdvisoryLocks sends is `SELECT pg_advisory_xact_lock($1)`,
// matched here the same way waitForBlockedSyncRunsLock matches LockRun's
// row-lock SELECT. Each test using this starts its own dedicated Postgres
// container (startFeatureDisabledPool), so there is no cross-test
// contamination to disambiguate by lock key.
func waitForBlockedAdvisoryLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, waiterDescription string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'
			  AND query LIKE '%pg_advisory_xact_lock%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never blocked on the held bucket advisory lock", waiterDescription)
}

// waitForBlockedSyncRunsLock polls pg_stat_activity, deterministically (no
// sleep-and-hope): it returns the instant Postgres itself reports a
// session waiting on syncrunrollup.LockRun's own SELECT ... FOR UPDATE
// statement against sync_runs, or fails the test after a bounded deadline
// if that never happens. Same technique as
// native_dispatch_sync_run_rollback_fence_integration_test.go's own
// waitForBlockedOutboxLock.
func waitForBlockedSyncRunsLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, waiterDescription string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query LIKE '%FROM public.sync_runs WHERE id = $1 FOR UPDATE%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never blocked on the held sync_runs row lock", waiterDescription)
}

// TestTerminalizeFeatureDisabledRunLeavesGenuinelyNonterminalStateAlone pins
// the guard terminalizeFeatureDisabledPlan relies on: a unit sitting outside
// every status terminalizeFeatureDisabledRun knows how to close out (a
// defensive path -- the production status enum only has five values, but
// nothing at the SQL layer here enforces that) must leave RunTerminal
// false, and terminalizeFeatureDisabledPlan must refuse to proceed to graph
// termination in that case rather than silently treating a stuck run as
// done.
func TestTerminalizeFeatureDisabledRunLeavesGenuinelyNonterminalStateAlone(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	stuck := "00000000-0000-4000-8000-0000000001b1"
	insertFeatureDisabledUnit(t, ctx, pool, stuck, "some_unmodeled_status", nil)

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		_, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC())
		if !errors.Is(err, ErrFeatureDisabledPlanNotTerminal) {
			t.Fatalf("terminalizeFeatureDisabledPlan error=%v want=ErrFeatureDisabledPlanNotTerminal", err)
		}
	})
}

// TestTerminalizeFeatureDisabledGraphTerminatesLedgerOutboxAndObservers pins
// _terminalize_feature_disabled_graph end to end: the reference-discovery
// ledger goes FAILED, every pending outbox row for the run is dispatched
// with the feature_disabled sentinel, a finalize_sync_run outbox row is
// created in that same terminal state when none existed, and
// observeTerminalSyncRun's BackfillJob/JobRun side effects fire.
func TestTerminalizeFeatureDisabledGraphTerminatesLedgerOutboxAndObservers(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001c1", "planned", nil)

	ledgerID := "00000000-0000-4000-8000-0000000001c2"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, available_at)
VALUES ($1, $2, $3, 'planned', now())`, ledgerID, featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	dispatchOutboxID := "00000000-0000-4000-8000-0000000001c3"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id, sync_run_id, org_id, kind, status, available_at, created_at, updated_at)
VALUES ($1, $2, $3, 'dispatch_sync_run', 'pending', now(), now(), now())`,
		dispatchOutboxID, featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	jobID := "00000000-0000-4000-8000-0000000001c4"
	if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id) VALUES ($1)`, jobID); err != nil {
		t.Fatal(err)
	}
	jobRunID := "00000000-0000-4000-8000-0000000001c5"
	if _, err := pool.Exec(ctx, `
INSERT INTO job_runs (id, job_id, status, result) VALUES ($1, $2, $3, $4::json)`,
		jobRunID, jobID, jobRunStatusRunning, `{"sync_run_id":"`+featureDisabledTestRun+`"}`); err != nil {
		t.Fatal(err)
	}
	backfillID := "00000000-0000-4000-8000-0000000001c6"
	if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, celery_task_id, status) VALUES ($1, $2, $3, 'running')`,
		backfillID, featureDisabledTestOrg, "sync_run:"+featureDisabledTestRun); err != nil {
		t.Fatal(err)
	}

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		if _, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC()); err != nil {
			t.Fatalf("terminalizeFeatureDisabledPlan: %v", err)
		}
	})

	var ledgerStatus string
	var ledgerError *string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_reference_discoveries WHERE id=$1`, ledgerID).
		Scan(&ledgerStatus, &ledgerError); err != nil {
		t.Fatal(err)
	}
	if ledgerStatus != discoveryStatusFailed {
		t.Fatalf("ledger status=%q want=%q", ledgerStatus, discoveryStatusFailed)
	}
	if ledgerError == nil || !strings.Contains(*ledgerError, "canonical incident ingestion is disabled") {
		t.Fatalf("ledger error=%v want sanitized feature-disabled message", ledgerError)
	}

	var dispatchStatus, dispatchLastError string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE id=$1`, dispatchOutboxID).
		Scan(&dispatchStatus, &dispatchLastError); err != nil {
		t.Fatal(err)
	}
	if dispatchStatus != "dispatched" || dispatchLastError != featureDisabledErrorCategory {
		t.Fatalf("dispatch_sync_run outbox status=%q last_error=%q want dispatched/%s", dispatchStatus, dispatchLastError, featureDisabledErrorCategory)
	}

	var finalizeCount int
	var finalizeStatus, finalizeLastError string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeStatus, &finalizeLastError); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeCount); err != nil {
		t.Fatal(err)
	}
	if finalizeCount != 1 {
		t.Fatalf("finalize_sync_run outbox rows=%d want=1", finalizeCount)
	}
	if finalizeStatus != "dispatched" || finalizeLastError != featureDisabledErrorCategory {
		t.Fatalf("finalize outbox status=%q last_error=%q want dispatched/%s", finalizeStatus, finalizeLastError, featureDisabledErrorCategory)
	}

	var jobRunStatus int
	if err := pool.QueryRow(ctx, `SELECT status FROM job_runs WHERE id=$1`, jobRunID).Scan(&jobRunStatus); err != nil {
		t.Fatal(err)
	}
	if jobRunStatus != jobRunStatusFailed {
		t.Fatalf("job_runs.status=%d want=%d (failed)", jobRunStatus, jobRunStatusFailed)
	}
	var backfillStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM backfill_jobs WHERE id=$1`, backfillID).Scan(&backfillStatus); err != nil {
		t.Fatal(err)
	}
	if backfillStatus != "failed" {
		t.Fatalf("backfill_jobs.status=%q want=failed", backfillStatus)
	}
}

// TestTerminalizeFeatureDisabledGraphUpsertsAnExistingFinalizeRow pins the
// ON CONFLICT (sync_run_id, kind) upsert path: a finalize_sync_run outbox
// row that already exists (in any prior state) is overwritten in place,
// never duplicated -- mirroring Python's find-or-create-else-update branch.
func TestTerminalizeFeatureDisabledGraphUpsertsAnExistingFinalizeRow(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001d1", "planned", nil)
	existingFinalizeID := "00000000-0000-4000-8000-0000000001d2"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id, sync_run_id, org_id, kind, status, available_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'pending', now(), now(), now())`,
		existingFinalizeID, featureDisabledTestRun, featureDisabledTestOrg, outboxKindFinalizeSyncRun); err != nil {
		t.Fatal(err)
	}

	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run := &finalizeSyncRun{id: featureDisabledTestRun, orgID: featureDisabledTestOrg}
		if _, err := terminalizeFeatureDisabledPlan(ctx, tx, run, scheduledsync.FeatureDecisionReasonGlobalDisabled, time.Now().UTC()); err != nil {
			t.Fatalf("terminalizeFeatureDisabledPlan: %v", err)
		}
	})

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("finalize outbox rows=%d want=1 (upsert must not duplicate)", count)
	}
	var id, status string
	if err := pool.QueryRow(ctx, `SELECT id::text, status FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&id, &status); err != nil {
		t.Fatal(err)
	}
	if id != existingFinalizeID {
		t.Fatalf("finalize outbox row id=%s want the pre-existing row %s reused, not replaced", id, existingFinalizeID)
	}
	if status != "dispatched" {
		t.Fatalf("finalize outbox status=%q want=dispatched", status)
	}
}

// TestArmFeatureDisabledFinalizeIsRaceSafeAndIdempotent pins
// _arm_feature_disabled_finalize's bool return: true only on the call that
// actually creates the row.
func TestArmFeatureDisabledFinalizeIsRaceSafeAndIdempotent(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)

	var firstArmed, secondArmed bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		firstArmed, err = armFeatureDisabledFinalize(ctx, tx, featureDisabledTestOrg, featureDisabledTestRun, time.Now().UTC())
		if err != nil {
			t.Fatalf("first armFeatureDisabledFinalize: %v", err)
		}
	})
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		secondArmed, err = armFeatureDisabledFinalize(ctx, tx, featureDisabledTestOrg, featureDisabledTestRun, time.Now().UTC())
		if err != nil {
			t.Fatalf("second armFeatureDisabledFinalize: %v", err)
		}
	})
	if !firstArmed {
		t.Fatal("first call armed=false want=true")
	}
	if secondArmed {
		t.Fatal("second call armed=true want=false (row already exists)")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
		featureDisabledTestRun, outboxKindFinalizeSyncRun).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("finalize outbox rows=%d want=1", count)
	}
}

// TestSyncRunRequiresCanonicalIncidentFeatureUnitScopeWins pins unit-scope
// precedence: pagerduty/incidents legacy-targets to "operational" (a gated
// target), so a run with a unit already planned against it requires the
// feature regardless of the integration's own dataset configuration.
func TestSyncRunRequiresCanonicalIncidentFeatureUnitScopeWins(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, provider, dataset_key, status)
VALUES ('00000000-0000-4000-8000-0000000001e1', $1, $2, 'pagerduty', 'incidents', 'planned')`,
		featureDisabledTestOrg, featureDisabledTestRun); err != nil {
		t.Fatal(err)
	}
	// An integration-level scope that would NOT require the feature, proving
	// the unit scope -- not this -- decided the answer.
	if _, err := pool.Exec(ctx, `
INSERT INTO integration_datasets (id, integration_id, dataset_key, is_enabled)
VALUES ('00000000-0000-4000-8000-0000000001e2', $1, 'users', true)`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}

	var requires bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if !requires {
		t.Fatal("requires=false want=true (pagerduty/incidents unit scope is gated)")
	}
}

// TestSyncRunRequiresCanonicalIncidentFeatureIntegrationFallbackRespectsIsEnabled
// pins the no-units-yet fallback path AND that a disabled integration
// dataset is excluded, matching IntegrationDataset.is_enabled.is_(True) in
// the Python query.
func TestSyncRunRequiresCanonicalIncidentFeatureIntegrationFallbackRespectsIsEnabled(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 0)
	if _, err := pool.Exec(ctx, `
INSERT INTO integration_datasets (id, integration_id, dataset_key, is_enabled)
VALUES ('00000000-0000-4000-8000-0000000001f1', $1, 'incidents', false)`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE integrations SET provider = 'pagerduty' WHERE id = $1`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}

	var requires bool
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if requires {
		t.Fatal("requires=true want=false (the only configured dataset scope is disabled)")
	}

	if _, err := pool.Exec(ctx, `UPDATE integration_datasets SET is_enabled = true WHERE integration_id = $1`, featureDisabledTestIntegration); err != nil {
		t.Fatal(err)
	}
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		requires, err = syncRunRequiresCanonicalIncidentFeature(ctx, tx, featureDisabledTestRun, featureDisabledTestIntegration)
		if err != nil {
			t.Fatalf("syncRunRequiresCanonicalIncidentFeature: %v", err)
		}
	})
	if !requires {
		t.Fatal("requires=false want=true once the pagerduty/incidents dataset scope is enabled")
	}
}
