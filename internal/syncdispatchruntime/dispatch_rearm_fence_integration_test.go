//go:build integration

package syncdispatchruntime

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// Re-arm fence cells: the re-arm a pass writes after its commit, against every state the run's dispatch row can be
// in by then (row state x the row's attempt against the pass's delivery attempt). An older pass must not move a row
// a newer delivery owns, and its upsert must not clear a live relay claim. One run, one bucket
// (github/commits/rest_core), a live running unit R and planned units.

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

// seedRearmFenceRun seeds the delivered row at attempt, R running under a live lease, and the planned units.
func seedRearmFenceRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64, planned ...string) {
	t.Helper()
	seedDispatchRoute(t, ctx, pool)
	markReferenceDiscoverySucceeded(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = $2 WHERE id = $1`, dispatchServiceTestOutbox, attempt); err != nil {
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
	claimToken  *string
}

func readRearmFenceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) rearmFenceRow {
	t.Helper()
	var row rearmFenceRow
	if err := pool.QueryRow(ctx, `SELECT status, attempts, available_at, claim_token FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
		discoveryTestRun, outboxKindDispatchSyncRun).Scan(&row.status, &row.attempts, &row.availableAt, &row.claimToken); err != nil {
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

// TestRearmFenceOlderPassKeepsANewerDelivery (D1): the pass at attempt 1 queues one unit and leaves one capped. After
// its commit and before its re-arm, a lease repair turns R into a retry and wakes the run, and the relay delivers
// that wake at attempt 2. The old pass's re-arm must leave the row the attempt-2 delivery owns, and that delivery
// must run its pass (it claims the capped unit, whose slot R freed).
func TestRearmFenceOlderPassKeepsANewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		seedRearmFenceRun(t, ctx, pool, 1, rearmFencePlannedA, rearmFencePlannedB)
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
		seedRearmFenceRun(t, ctx, pool, 1, rearmFencePlannedA, rearmFencePlannedB)
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

// relayClaims stands in for the relay claim (syncreconciler/kernel.go claimRiverRoutesSQL) of a pending row: the
// next attempt, a live claim until expiresAt.
func relayClaims(t *testing.T, ctx context.Context, pool *pgxpool.Pool, expiresAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET claim_token = gen_random_uuid()::text, claim_expires_at = $2, claim_transport = 'river',
    claim_route_generation = $3, attempts = attempts + 1, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending'`, discoveryTestRun, expiresAt, dispatchRouteGeneration); err != nil {
		t.Fatal(err)
	}
}

func wakeRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, at time.Time) {
	t.Helper()
	if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, at, syncrunrollup.DispatchWakePathProviderUnit) {
		t.Fatal("precondition: the wake did not arm")
	}
}

// TestRearmFenceOlderPassWakeKeepsANewerDelivery (D1, the pass's own deferred wake): the pass at attempt 1 claims
// two unroutable units of a medium bucket (cap 2) and terminalizes them, so it defers
// syncrunrollup.WakeDispatch (path dispatch_pass) for its own run; the valid sibling stays planned. After its commit
// and before its re-arm and wake, a unit end elsewhere wakes the run and the relay delivers that wake at attempt 2.
// Neither post-commit write of the old pass (scheduleRedispatch, then the deferred wake) may move the row the
// attempt-2 delivery owns, and that delivery must run its pass (it claims the sibling).
func TestRearmFenceOlderPassWakeKeepsANewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}
		for _, unit := range []struct{ id, dataset string }{
			{"00000000-0000-4000-8000-0000001447b0", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447b1", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447b2", "commits"},
		} {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$4,$5,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
				unit.id, discoveryTestOrg, discoveryTestRun, unit.dataset, dispatchTestSource); err != nil {
				t.Fatal(err)
			}
		}

		old := newTestDispatchService(t, pool)
		var newerAttempt int64
		hook := installAfterPassCommit(t, old, func() {
			wakeRun(t, ctx, pool, time.Now().UTC())
			newerAttempt = relayDelivers(t, ctx, pool)
		})
		armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathDispatchPass, syncrunrollup.DispatchWakeOutcomeArmed)
		if err := old.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if !hook.fired.Load() || newerAttempt != 2 {
			t.Fatalf("precondition: interleave fired=%v newer attempt=%d, want true/2", hook.fired.Load(), newerAttempt)
		}
		if failed := unitIDsInStatus(t, ctx, pool, "failed"); len(failed) != 2 {
			t.Fatalf("precondition: %d units terminalized in the old pass, want the 2 unroutable ones (%v)", len(failed), failed)
		}
		if planned := unitIDsInStatus(t, ctx, pool, "planned"); len(planned) != 1 {
			t.Fatalf("precondition: %d planned units, want the 1 sibling the cap held back (%v)", len(planned), planned)
		}
		if got := providerUnitPublishes(t, ctx, pool); got != 0 {
			t.Fatalf("precondition: old pass published %d units, want 0", got)
		}
		t.Logf("dispatch_pass wake armed counter moved %d", syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathDispatchPass, syncrunrollup.DispatchWakeOutcomeArmed)-armedBefore)

		row := readRearmFenceRow(t, ctx, pool)
		t.Logf("after old pass: status=%s attempts=%d available_at=%s", row.status, row.attempts, row.availableAt)
		if row.status != "dispatched" || row.attempts != 2 {
			t.Errorf("row status=%s attempts=%d, want dispatched/2: a post-commit write of the attempt-1 pass moved the row the attempt-2 delivery owns", row.status, row.attempts)
		}

		newer := newTestDispatchService(t, pool)
		if err := newer.Dispatch(ctx, rearmFenceArgs(2)); err != nil {
			t.Fatalf("newer delivery: %v, want nil", err)
		}
		if got := providerUnitPublishes(t, ctx, pool); got != 1 {
			t.Errorf("published %d units after the attempt-2 delivery, want 1: the newer delivery was refused, its pass never ran", got)
		}
	})
}

// TestRearmFenceOlderPassKeepsALiveRelayClaim (D1b): the pass at attempt 1 queues one unit and leaves one capped, so
// it re-arms at armAt + the 60 s countdown. After its commit and before its re-arm, a unit end wakes the run and the
// relay claims that wake (attempt 2, claim live for 10 s). The re-arm must not clear that claim: the claim is live
// now, and only its expiry against the re-arm's far target makes it look dead.
func TestRearmFenceOlderPassKeepsALiveRelayClaim(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		seedRearmFenceRun(t, ctx, pool, 1, rearmFencePlannedA, rearmFencePlannedB)

		old := newTestDispatchService(t, pool)
		var claimed rearmFenceRow
		hook := installAfterPassCommit(t, old, func() {
			hookAt := time.Now().UTC()
			wakeRun(t, ctx, pool, hookAt)
			relayClaims(t, ctx, pool, hookAt.Add(10*time.Second))
			claimed = readRearmFenceRow(t, ctx, pool)
		})
		if err := old.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if !hook.fired.Load() || claimed.status != "pending" || claimed.attempts != 2 || claimed.claimToken == nil {
			t.Fatalf("precondition: interleave fired=%v claimed row %s/%d claim=%v, want true pending/2 claim set",
				hook.fired.Load(), claimed.status, claimed.attempts, claimed.claimToken != nil)
		}
		if got := providerUnitPublishes(t, ctx, pool); got != 1 {
			t.Fatalf("precondition: old pass published %d units, want 1 (one slot free, one unit capped)", got)
		}

		row := readRearmFenceRow(t, ctx, pool)
		t.Logf("after old re-arm: status=%s attempts=%d available_at=%s claim=%v", row.status, row.attempts, row.availableAt, row.claimToken != nil)
		if row.status != "pending" || row.attempts != 2 || row.claimToken == nil || *row.claimToken != *claimed.claimToken {
			t.Errorf("row %s/%d claim=%v, want pending/2 with the relay's claim kept: the re-arm cleared a claim that is live now",
				row.status, row.attempts, row.claimToken != nil)
		}
	})
}

// onLogMessage is a slog handler that runs fn once, at the first record with the given message.
type onLogMessage struct {
	message string
	fired   *atomic.Bool
	fn      func()
}

func (handler onLogMessage) Enabled(context.Context, slog.Level) bool { return true }
func (handler onLogMessage) Handle(_ context.Context, record slog.Record) error {
	if record.Message == handler.message && handler.fired.CompareAndSwap(false, true) {
		handler.fn()
	}
	return nil
}
func (handler onLogMessage) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler onLogMessage) WithGroup(string) slog.Handler      { return handler }

// TestRearmFenceOlderPassOwnWakeAloneUndoesNoNewerDelivery isolates the pass's deferred own wake
// (syncrunrollup.WakeDispatch, path dispatch_pass, native_dispatch_sync_run_service.go:616) from its re-arm: the
// interleave of TestRearmFenceOlderPassWakeKeepsANewerDelivery, and at the pass's dispatch_sync_run.noop line (after
// scheduleRedispatch returned, before the deferred wake runs) the row is put back to dispatched/2 if the re-arm moved
// it. Only the own wake can then write the row; it must leave the row the attempt-2 delivery owns.
func TestRearmFenceOlderPassOwnWakeAloneUndoesNoNewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}
		for _, unit := range []struct{ id, dataset string }{
			{"00000000-0000-4000-8000-0000001447c0", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447c1", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447c2", "commits"},
		} {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$4,$5,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
				unit.id, discoveryTestOrg, discoveryTestRun, unit.dataset, dispatchTestSource); err != nil {
				t.Fatal(err)
			}
		}

		old := newTestDispatchService(t, pool)
		var newerAttempt int64
		hook := installAfterPassCommit(t, old, func() {
			wakeRun(t, ctx, pool, time.Now().UTC())
			newerAttempt = relayDelivers(t, ctx, pool)
		})
		var afterRearm rearmFenceRow
		var noopSeen atomic.Bool
		old.logger = synclog.New(slog.New(onLogMessage{message: "dispatch_sync_run.noop", fired: &noopSeen, fn: func() {
			afterRearm = readRearmFenceRow(t, ctx, pool)
			if afterRearm.status == "dispatched" && afterRearm.attempts == 2 {
				return
			}
			if _, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL,
    claim_route_generation = NULL, last_error = NULL, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND attempts = 2`, discoveryTestRun, dispatchRouteGeneration); err != nil {
				t.Errorf("put the row back to dispatched/2: %v", err)
			}
		}}))
		if err := old.Dispatch(ctx, rearmFenceArgs(1)); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if !hook.fired.Load() || newerAttempt != 2 || !noopSeen.Load() {
			t.Fatalf("precondition: interleave fired=%v newer attempt=%d noop line seen=%v, want true/2/true", hook.fired.Load(), newerAttempt, noopSeen.Load())
		}
		if failed := unitIDsInStatus(t, ctx, pool, "failed"); len(failed) != 2 {
			t.Fatalf("precondition: %d units terminalized in the old pass, want the 2 unroutable ones (%v)", len(failed), failed)
		}
		t.Logf("row after the re-arm (before put-back): %s/%d", afterRearm.status, afterRearm.attempts)

		row := readRearmFenceRow(t, ctx, pool)
		t.Logf("row after the own wake: status=%s attempts=%d available_at=%s", row.status, row.attempts, row.availableAt)
		if row.status != "dispatched" || row.attempts != 2 {
			t.Errorf("row status=%s attempts=%d, want dispatched/2: the attempt-1 pass's own deferred wake moved the row the attempt-2 delivery owns", row.status, row.attempts)
		}
	})
}
