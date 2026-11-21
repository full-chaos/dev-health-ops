//go:build integration

package syncdispatchruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// Re-arm fence cells: an older pass must not move a row a newer delivery owns, a failed re-arm must not end the job
// as a success with no wake, and a retry of a committed attempt must not admit work that became admissible after
// that commit. One run, one bucket (github/commits/rest_core), a live running unit R and planned units.

const (
	rearmFenceRunning  = "00000000-0000-4000-8000-0000001447a0"
	rearmFencePlannedA = "00000000-0000-4000-8000-0000001447a1"
	rearmFencePlannedB = "00000000-0000-4000-8000-0000001447a2"
)

// rearmFenceArgs is dispatchTestArgs at a given outbox attempt.
func rearmFenceArgs(attempt int64) DispatchSyncRunArgs {
	args := dispatchTestArgs()
	args.DeliveryAttempt = attempt
	return args
}

// seedRearmFenceRun seeds the delivered row at attempt 1, R running under a live lease, and the planned units.
func seedRearmFenceRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, planned ...string) {
	t.Helper()
	seedDispatchRoute(t, ctx, pool)
	markReferenceDiscoverySucceeded(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
		t.Fatal(err)
	}
	now := pgNow()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,lease_owner,lease_expires_at,last_heartbeat_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'running',$5,'live-worker',$6,$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',1,now())`,
		rearmFenceRunning, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, id := range planned {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'planned',$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',0,now())`,
			id, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now); err != nil {
			t.Fatal(err)
		}
	}
}

// afterPassCommit runs fn once, inside the first service clock read after the pass's fenced commit. The commit
// snapshot's clock (commitSnapshotClock) is read only inside the fenced commit, and no service clock read sits
// between that snapshot and COMMIT, so the first service read after it is the first one after the commit: the
// post-commit re-arm (its armAt) or the tail's pending-counts read.
type afterPassCommit struct {
	snapshotTaken atomic.Bool
	fired         atomic.Bool
}

func installAfterPassCommit(t *testing.T, service *NativeDispatchSyncRunService, fn func()) *afterPassCommit {
	t.Helper()
	hook := &afterPassCommit{}
	saved := commitSnapshotClock
	t.Cleanup(func() { commitSnapshotClock = saved })
	commitSnapshotClock.now = func() time.Time {
		hook.snapshotTaken.Store(true)
		return time.Now()
	}
	service.now = func() time.Time {
		if hook.snapshotTaken.Load() && hook.fired.CompareAndSwap(false, true) {
			fn()
		}
		return time.Now()
	}
	return hook
}

// lockTimeoutPool is a second pool on the same database whose sessions give up a lock wait after timeout, so a held
// lock fails the service's statement at once instead of blocking it.
func lockTimeoutPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, timeout string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["lock_timeout"] = timeout
	limited, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(limited.Close)
	return limited
}

// leaseRepairRetries stands in for LeaseRepair.Step on R (running -> retrying, a deferral earlier than the
// countdown) and runs the real post-commit wake it arms (syncrunrollup.WakeDispatch, path lease_repair).
func leaseRepairRetries(t *testing.T, ctx context.Context, pool *pgxpool.Pool, retryAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE sync_run_units SET status = 'retrying', available_at = $2, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = $1::uuid AND status = 'running'`, rearmFenceRunning, retryAt); err != nil {
		t.Fatal(err)
	}
	if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, time.Now().UTC(), syncrunrollup.DispatchWakePathLeaseRepair) {
		t.Fatal("precondition: the lease-repair wake did not arm")
	}
}

// relayDelivers stands in for the reconciler relay's claim + mark (syncreconciler/kernel.go claimRiverRoutesSQL
// bumps attempts, markRiverDispatchedSQL moves the row to 'dispatched'): the newer delivery's attempt.
func relayDelivers(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var attempt int64
	if err := pool.QueryRow(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', attempts = attempts + 1, dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL,
    claim_route_generation = NULL, last_error = NULL, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending' AND claim_token IS NULL
RETURNING attempts`, discoveryTestRun, dispatchRouteGeneration).Scan(&attempt); err != nil {
		t.Fatalf("relay stand-in: %v", err)
	}
	return attempt
}

type rearmFenceRow struct {
	status      string
	attempts    int64
	availableAt time.Time
}

func readRearmFenceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) rearmFenceRow {
	t.Helper()
	var row rearmFenceRow
	if err := pool.QueryRow(ctx, `SELECT status, attempts, available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
		discoveryTestRun, outboxKindDispatchSyncRun).Scan(&row.status, &row.attempts, &row.availableAt); err != nil {
		t.Fatal(err)
	}
	return row
}

func providerUnitPublishes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = $1`, jobcontract.KindSyncProviderUnit).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func rearmFenceUnitStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// holdInTx opens a transaction on pool, runs statement in it and keeps it open until release.
func holdInTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, statement string, args ...any) (release func()) {
	t.Helper()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, statement, args...); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback(ctx) }
}

// TestRearmFenceOlderPassKeepsANewerDelivery (D1): the pass at attempt 1 queues one unit and leaves one capped. After
// its commit and before its re-arm, a lease repair turns R into a retry and wakes the run, and the relay delivers
// that wake at attempt 2. The old pass's re-arm must leave the row the attempt-2 delivery owns, and that delivery
// must run its pass (it claims the capped unit, whose slot R freed).
func TestRearmFenceOlderPassKeepsANewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		seedRearmFenceRun(t, ctx, pool, rearmFencePlannedA, rearmFencePlannedB)
		retryAt := pgNow().Add(30 * time.Second)

		old := newTestDispatchService(t, pool)
		var newerAttempt int64
		hook := installAfterPassCommit(t, old, func() {
			leaseRepairRetries(t, ctx, pool, retryAt)
			newerAttempt = relayDelivers(t, ctx, pool)
		})
		if err := old.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if !hook.fired.Load() || newerAttempt != 2 {
			t.Fatalf("precondition: interleave fired=%v newer attempt=%d, want true/2", hook.fired.Load(), newerAttempt)
		}
		if got := providerUnitPublishes(t, ctx, pool); got != 1 {
			t.Fatalf("precondition: old pass published %d units, want 1 (one slot free, one unit capped)", got)
		}

		row := readRearmFenceRow(t, ctx, pool)
		t.Logf("after old re-arm: status=%s attempts=%d available_at=%s retry_at=%s", row.status, row.attempts, row.availableAt, retryAt)
		if row.status != "dispatched" || row.attempts != 2 {
			t.Errorf("row status=%s attempts=%d, want dispatched/2: the attempt-1 pass's re-arm moved the row the attempt-2 delivery owns", row.status, row.attempts)
		}

		newer := newTestDispatchService(t, pool)
		if err := newer.Dispatch(ctx, rearmFenceArgs(2)); err != nil {
			t.Fatalf("newer delivery: %v, want nil", err)
		}
		if got := providerUnitPublishes(t, ctx, pool); got != 2 {
			t.Errorf("published %d units after the attempt-2 delivery, want 2: the newer delivery was refused, its pass never ran", got)
		}
	})
}

// TestRearmFenceOlderPassKeepsAWakeArmedMidPass is D1's control: the same interleave without the relay delivery. The
// row is pending (a wake another writer armed) at the same attempt, so the re-arm keeps the earlier wake.
func TestRearmFenceOlderPassKeepsAWakeArmedMidPass(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		seedRearmFenceRun(t, ctx, pool, rearmFencePlannedA, rearmFencePlannedB)
		retryAt := pgNow().Add(30 * time.Second)

		old := newTestDispatchService(t, pool)
		var wokenAt time.Time
		hook := installAfterPassCommit(t, old, func() {
			leaseRepairRetries(t, ctx, pool, retryAt)
			wokenAt = readRearmFenceRow(t, ctx, pool).availableAt
		})
		if err := old.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if !hook.fired.Load() {
			t.Fatal("precondition: the interleave did not fire")
		}
		row := readRearmFenceRow(t, ctx, pool)
		if row.status != "pending" || row.attempts != 1 || !row.availableAt.Equal(wokenAt) {
			t.Fatalf("row status=%s attempts=%d available_at=%s, want pending/1/%s: the re-arm must keep a wake armed mid-pass", row.status, row.attempts, row.availableAt, wokenAt)
		}
	})
}

// rearmFailurePass runs the D2 pass: one unit queued, one capped, so the pass re-arms after its commit. When
// holdRow is set, another transaction holds the run's outbox row from the commit on, and the service's lock_timeout
// fails the re-arm's row lock.
func rearmFailurePass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, holdRow bool) (rearmFenceRow, error) {
	t.Helper()
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
	seedRearmFenceRun(t, ctx, pool, rearmFencePlannedA, rearmFencePlannedB)
	service := newTestDispatchService(t, lockTimeoutPool(t, ctx, pool, "300ms"))
	release := func() {}
	hook := installAfterPassCommit(t, service, func() {
		if holdRow {
			release = holdInTx(t, ctx, pool, `SELECT 1 FROM sync_dispatch_outbox WHERE id = $1 FOR UPDATE`, dispatchServiceTestOutbox)
		}
	})
	err := service.Dispatch(ctx, rearmFenceArgs(1))
	release()
	if !hook.fired.Load() {
		t.Fatal("precondition: the post-commit hook did not fire")
	}
	if got := providerUnitPublishes(t, ctx, pool); got != 1 {
		t.Fatalf("precondition: the pass published %d units, want 1 (its commit landed)", got)
	}
	return readRearmFenceRow(t, ctx, pool), err
}

// TestRearmFenceFailedRearmIsNotASuccessWithNoWake (D2): the pass committed and its re-arm write failed. The job must
// not end as a success with the row still 'dispatched' and no pending wake for the capped unit.
func TestRearmFenceFailedRearmIsNotASuccessWithNoWake(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		row, err := rearmFailurePass(t, ctx, pool, true)
		t.Logf("dispatch err=%v row status=%s attempts=%d available_at=%s", err, row.status, row.attempts, row.availableAt)
		if err == nil && row.status != "pending" {
			t.Errorf("Dispatch returned nil with row status=%s: a failed re-arm ended the job as a success with no pending wake", row.status)
		}
	})
}

// TestRearmFenceRearmArmsAWakeWhenTheWriteSucceeds is D2's control: no held row, the re-arm lands.
func TestRearmFenceRearmArmsAWakeWhenTheWriteSucceeds(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		row, err := rearmFailurePass(t, ctx, pool, false)
		if err != nil || row.status != "pending" || row.attempts != 1 {
			t.Fatalf("Dispatch err=%v row status=%s attempts=%d, want nil/pending/1", err, row.status, row.attempts)
		}
	})
}

// retryAfterCommittedPass runs the D3 sequence: a zero-queue pass (the one slot is R's) commits, its tail
// pending-counts read fails (another transaction holds sync_run_units, the service's lock_timeout gives up), the
// job returns an error, then freeSlot (R ends) or not, then River's retry of the SAME attempt.
func retryAfterCommittedPass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, freeSlot bool) {
	t.Helper()
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
	seedRearmFenceRun(t, ctx, pool, rearmFencePlannedA)
	service := newTestDispatchService(t, lockTimeoutPool(t, ctx, pool, "300ms"))
	release := func() {}
	hook := installAfterPassCommit(t, service, func() {
		release = holdInTx(t, ctx, pool, `LOCK TABLE sync_run_units IN ACCESS EXCLUSIVE MODE`)
	})
	first := service.Dispatch(ctx, rearmFenceArgs(1))
	release()
	if !hook.fired.Load() || first == nil {
		t.Fatalf("precondition: hook fired=%v first err=%v, want true/non-nil (the tail read after the commit failed)", hook.fired.Load(), first)
	}
	if got := providerUnitPublishes(t, ctx, pool); got != 0 {
		t.Fatalf("precondition: the first pass published %d units, want 0 (the bucket is full)", got)
	}
	if row := readRearmFenceRow(t, ctx, pool); row.status != "dispatched" || row.attempts != 1 {
		t.Fatalf("precondition: row status=%s attempts=%d, want dispatched/1 (the retry passes the start fence)", row.status, row.attempts)
	}
	if freeSlot {
		if _, err := pool.Exec(ctx, `
UPDATE sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = $1::uuid`, rearmFenceRunning); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
		t.Fatalf("retry: %v, want nil", err)
	}
}

// TestRearmFenceRetryOfACommittedAttemptAdmitsNothingNew (D3): between the failed attempt and its retry, R ends and
// frees the slot. The retry runs under the attempt whose pass already committed: it must not claim and publish the
// planned unit under that attempt; it must leave a wake so the next delivery admits it. This asserts the completion
// marker option; a ruling that a retry may admit makes this cell a declared-behavior cell instead.
func TestRearmFenceRetryOfACommittedAttemptAdmitsNothingNew(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		retryAfterCommittedPass(t, ctx, pool, true)
		published := providerUnitPublishes(t, ctx, pool)
		status := rearmFenceUnitStatus(t, ctx, pool, rearmFencePlannedA)
		row := readRearmFenceRow(t, ctx, pool)
		t.Logf("after retry: published=%d unit=%s row status=%s attempts=%d", published, status, row.status, row.attempts)
		if published != 0 || status != syncRunUnitStatusPlanned {
			t.Errorf("retry of committed attempt 1 published %d units and left the unit %s, want 0/planned", published, status)
		}
		if row.status != "pending" {
			t.Errorf("row status=%s after the retry, want pending: the unit the retry did not admit has no wake", row.status)
		}
	})
}

// TestRearmFenceRetryWithNothingNewAdmitsNothing is D3's control: the slot stays full, the retry admits nothing.
func TestRearmFenceRetryWithNothingNewAdmitsNothing(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		retryAfterCommittedPass(t, ctx, pool, false)
		if published, status := providerUnitPublishes(t, ctx, pool), rearmFenceUnitStatus(t, ctx, pool, rearmFencePlannedA); published != 0 || status != syncRunUnitStatusPlanned {
			t.Fatalf("retry published %d units and left the unit %s, want 0/planned", published, status)
		}
	})
}
