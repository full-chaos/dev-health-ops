//go:build integration

package syncdispatchruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
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

// The matrix below runs one pass at delivery attempt 2: one unit queued, one capped, so after its commit the pass
// re-arms the run at armAt + the 60 s countdown (main: the cappedIDs arm; #6804: queuedPassRearmAt with the capped
// unit dispatchable). setRow puts the row in the state under test between that commit and the re-arm.

const rearmMatrixAttempt = 2

func rearmMatrixPass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, setRow func(hookAt time.Time)) (before, after rearmFenceRow, hookAt time.Time) {
	t.Helper()
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
	seedRearmFenceRun(t, ctx, pool, rearmMatrixAttempt, rearmFencePlannedA, rearmFencePlannedB)
	service := newTestDispatchService(t, pool)
	hook := installAfterPassCommit(t, service, func() {
		hookAt = time.Now().UTC()
		if setRow != nil {
			setRow(hookAt)
		}
		before = readRearmFenceRow(t, ctx, pool)
	})
	if err := service.Dispatch(ctx, rearmFenceArgs(rearmMatrixAttempt)); err != nil {
		t.Fatalf("pass: %v, want nil", err)
	}
	if !hook.fired.Load() {
		t.Fatal("precondition: the post-commit hook did not fire")
	}
	if got := providerUnitPublishes(t, ctx, pool); got != 1 {
		t.Fatalf("precondition: the pass published %d units, want 1 (one slot free, one unit capped)", got)
	}
	after = readRearmFenceRow(t, ctx, pool)
	t.Logf("before re-arm: %s/%d/%s claim=%v; after: %s/%d/%s claim=%v", before.status, before.attempts, before.availableAt, before.claimToken != nil,
		after.status, after.attempts, after.availableAt, after.claimToken != nil)
	return before, after, hookAt
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

// relayPublishFails stands in for recordTransportPublishFailureSQL (syncreconciler/failure_backoff.go): the claim
// returns to pending at the relay's backoff.
func relayPublishFails(t *testing.T, ctx context.Context, pool *pgxpool.Pool, retryAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'pending', available_at = $2, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL,
    claim_route_generation = NULL, last_error = 'transport_publish_failed', updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND claim_token IS NOT NULL`, discoveryTestRun, retryAt); err != nil {
		t.Fatal(err)
	}
}

func wakeRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, at time.Time) {
	t.Helper()
	if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, at, syncrunrollup.DispatchWakePathProviderUnit) {
		t.Fatal("precondition: the wake did not arm")
	}
}

func assertRowUnchanged(t *testing.T, before, after rearmFenceRow, why string) {
	t.Helper()
	if after.status != before.status || after.attempts != before.attempts || !after.availableAt.Equal(before.availableAt) ||
		(after.claimToken == nil) != (before.claimToken == nil) || (after.claimToken != nil && *after.claimToken != *before.claimToken) {
		t.Errorf("row %s/%d/%s claim=%v -> %s/%d/%s claim=%v, want unchanged: %s", before.status, before.attempts, before.availableAt, before.claimToken != nil,
			after.status, after.attempts, after.availableAt, after.claimToken != nil, why)
	}
}

// TestRearmFenceMatrixOwnDispatchedRowCountsDown: dispatched at the pass's own attempt. WANT (as today): pending at
// the countdown, same attempt.
func TestRearmFenceMatrixOwnDispatchedRowCountsDown(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, hookAt := rearmMatrixPass(t, ctx, pool, nil)
		if before.status != "dispatched" || before.attempts != rearmMatrixAttempt {
			t.Fatalf("precondition: row %s/%d, want dispatched/%d", before.status, before.attempts, rearmMatrixAttempt)
		}
		if lag := after.availableAt.Sub(hookAt); after.status != "pending" || after.attempts != rearmMatrixAttempt || lag < 59*time.Second || lag > 65*time.Second {
			t.Fatalf("row %s/%d available_at=hook+%s, want pending/%d at hook+60s: the pass's own row counts down", after.status, after.attempts, lag, rearmMatrixAttempt)
		}
	})
}

// TestRearmFenceMatrixPendingSameAttemptKeepsTheEarlierWake: a wake (W4) moved the row to pending at the same
// attempt. WANT (as today, CHAOS-10013): the earlier wake stays.
func TestRearmFenceMatrixPendingSameAttemptKeepsTheEarlierWake(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(at time.Time) { wakeRun(t, ctx, pool, at) })
		if before.status != "pending" || before.attempts != rearmMatrixAttempt {
			t.Fatalf("precondition: row %s/%d, want pending/%d", before.status, before.attempts, rearmMatrixAttempt)
		}
		assertRowUnchanged(t, before, after, "a wake another writer armed is a real due time")
	})
}

// TestRearmFenceMatrixPendingHigherAttemptKeepsTheEarlierRetry: the relay claimed the row (attempt 3) and its
// publish failed (W3): pending, unclaimed, at the relay's backoff, which is earlier than the pass's countdown. WANT
// (as today): unchanged; no delivery runs attempt 3, the row is a wake like any other pending row.
func TestRearmFenceMatrixPendingHigherAttemptKeepsTheEarlierRetry(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(at time.Time) {
			wakeRun(t, ctx, pool, at)
			relayClaims(t, ctx, pool, at.Add(10*time.Minute))
			relayPublishFails(t, ctx, pool, at.Add(5*time.Second))
		})
		if before.status != "pending" || before.attempts != rearmMatrixAttempt+1 || before.claimToken != nil {
			t.Fatalf("precondition: row %s/%d claim=%v, want pending/%d unclaimed", before.status, before.attempts, before.claimToken != nil, rearmMatrixAttempt+1)
		}
		assertRowUnchanged(t, before, after, "the relay's earlier retry stays")
	})
}

// TestRearmFenceMatrixPendingHigherAttemptTakesTheEarlierOfRetryAndCountdown: as above, but the relay's backoff is
// later than the pass's countdown. WANT (as today, the upsert's LEAST): pending at the countdown, attempt 3.
func TestRearmFenceMatrixPendingHigherAttemptTakesTheEarlierOfRetryAndCountdown(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		_, after, hookAt := rearmMatrixPass(t, ctx, pool, func(at time.Time) {
			wakeRun(t, ctx, pool, at)
			relayClaims(t, ctx, pool, at.Add(10*time.Minute))
			relayPublishFails(t, ctx, pool, at.Add(5*time.Minute))
		})
		if lag := after.availableAt.Sub(hookAt); after.status != "pending" || after.attempts != rearmMatrixAttempt+1 || lag < 59*time.Second || lag > 65*time.Second {
			t.Fatalf("row %s/%d available_at=hook+%s, want pending/%d at hook+60s (the earlier of the two)", after.status, after.attempts, lag, rearmMatrixAttempt+1)
		}
	})
}

// TestRearmFenceMatrixPendingHigherAttemptKeepsALiveClaim: the relay holds a live claim at attempt 3 (it is about to
// deliver) that expires after the pass's target. WANT (as today): unchanged, the claim stays.
func TestRearmFenceMatrixPendingHigherAttemptKeepsALiveClaim(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(at time.Time) {
			wakeRun(t, ctx, pool, at)
			relayClaims(t, ctx, pool, at.Add(10*time.Minute))
		})
		if before.claimToken == nil || before.attempts != rearmMatrixAttempt+1 {
			t.Fatalf("precondition: claim=%v attempts=%d, want a live claim at %d", before.claimToken != nil, before.attempts, rearmMatrixAttempt+1)
		}
		assertRowUnchanged(t, before, after, "a live relay claim stays")
	})
}

// TestRearmFenceMatrixPendingHigherAttemptKeepsAClaimThatExpiresBeforeTheTarget (D1b): the relay's live claim
// expires after now but before the pass's target (now + 60 s). The upsert judges liveness against its own
// updated_at, which is the target, so it clears a claim that is live now and the relay's mark then fails. WANT:
// liveness judged at now; the claim stays.
func TestRearmFenceMatrixPendingHigherAttemptKeepsAClaimThatExpiresBeforeTheTarget(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(at time.Time) {
			wakeRun(t, ctx, pool, at)
			relayClaims(t, ctx, pool, at.Add(20*time.Second))
		})
		if before.claimToken == nil || before.attempts != rearmMatrixAttempt+1 {
			t.Fatalf("precondition: claim=%v attempts=%d, want a live claim at %d", before.claimToken != nil, before.attempts, rearmMatrixAttempt+1)
		}
		assertRowUnchanged(t, before, after, "a claim live NOW must not be cleared because it expires before the re-arm's target")
	})
}

// TestRearmFenceMatrixDispatchedHigherAttemptIsNotWritten (D1): the relay delivered attempt 3 before the re-arm.
// WANT: the row is not written; the attempt-3 delivery keeps it.
func TestRearmFenceMatrixDispatchedHigherAttemptIsNotWritten(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(at time.Time) {
			wakeRun(t, ctx, pool, at)
			relayDelivers(t, ctx, pool)
		})
		if before.status != "dispatched" || before.attempts != rearmMatrixAttempt+1 {
			t.Fatalf("precondition: row %s/%d, want dispatched/%d", before.status, before.attempts, rearmMatrixAttempt+1)
		}
		assertRowUnchanged(t, before, after, "a newer delivery owns the row")
	})
}

// TestRearmFenceMatrixDispatchedLowerAttemptIsNotWritten: dispatched at an attempt BELOW the pass's. No writer makes
// this state while the run exists (attempts only grows, kernel.go claim; rows are deleted only with their org), so
// it is set out of band here. WANT: not written: the pass cannot show the row is its own, and moving it would refuse
// whatever delivery holds it.
func TestRearmFenceMatrixDispatchedLowerAttemptIsNotWritten(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		before, after, _ := rearmMatrixPass(t, ctx, pool, func(time.Time) {
			if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = $2 WHERE id = $1`, dispatchServiceTestOutbox, rearmMatrixAttempt-1); err != nil {
				t.Fatal(err)
			}
		})
		if before.status != "dispatched" || before.attempts != rearmMatrixAttempt-1 {
			t.Fatalf("precondition: row %s/%d, want dispatched/%d", before.status, before.attempts, rearmMatrixAttempt-1)
		}
		assertRowUnchanged(t, before, after, "the pass does not own a row at another attempt")
	})
}

// TestRearmFenceReferenceDiscoverySuccessKeepsItsOwnInstant: the wakeup upsert's other dispatch_sync_run caller,
// reference discovery success, keeps its own instant: available_at, updated_at and the claim-liveness instant are its
// completion time, so a relay claim live at that time stays.
func TestRearmFenceReferenceDiscoverySuccessKeepsItsOwnInstant(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		completedAt := pgNow()
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,org_id,sync_run_id,kind,status,available_at,attempts,claim_token,claim_expires_at,claim_transport,claim_route_generation,created_at,updated_at)
VALUES ('00000000-0000-4000-8000-0000001447b0',$1,$2,'dispatch_sync_run','pending',$3,3,'relay-claim',$4,'river',$5,now(),now())`,
			discoveryTestOrg, discoveryTestRun, completedAt.Add(10*time.Minute), completedAt.Add(30*time.Second), dispatchRouteGeneration); err != nil {
			t.Fatal(err)
		}
		executor := &fakeDiscoveryExecutor{summary: map[string]any{"reference_team_keys": []string{"ENG"}}}
		service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
		if err != nil {
			t.Fatal(err)
		}
		service.now = func() time.Time { return completedAt }
		if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
			t.Fatalf("Discover: %v", err)
		}
		var status string
		var availableAt, updatedAt time.Time
		var claimToken *string
		if err := pool.QueryRow(ctx, `SELECT status, available_at, updated_at, claim_token FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'`,
			discoveryTestRun).Scan(&status, &availableAt, &updatedAt, &claimToken); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || !availableAt.Equal(completedAt) || !updatedAt.Equal(completedAt) || claimToken == nil || *claimToken != "relay-claim" {
			t.Fatalf("row %s available_at=%s updated_at=%s claim=%v, want pending at %s, updated %s, claim kept", status, availableAt, updatedAt, claimToken, completedAt, completedAt)
		}
	})
}
