//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// C1 and C3 of PR #6815 (CHAOS-14477, head ce932006615d), taken into CHAOS-14479: the atomic pass's re-arm runs
// under the dispatch row lock it took at the commit snapshot, so an older pass cannot move a row a newer delivery
// owns, nor clear a live relay claim. Same names and end-state assertions as #6815; the helpers carry the
// atomicFence prefix so #6815 can drop the 2 functions alone.

// atCommitPoint runs fn once, at the first service clock read after the pass's commit snapshot: on main that is after
// COMMIT (the post-commit re-arm or tail read); with the atomic pass it is in the tail, before the re-arm.
func atCommitPoint(t *testing.T, service *NativeDispatchSyncRunService, fn func()) *atomic.Bool {
	t.Helper()
	var snapshotTaken, fired atomic.Bool
	saved := commitSnapshotClock
	t.Cleanup(func() { commitSnapshotClock = saved })
	commitSnapshotClock.now = func() time.Time {
		snapshotTaken.Store(true)
		return time.Now()
	}
	service.now = func() time.Time {
		if snapshotTaken.Load() && fired.CompareAndSwap(false, true) {
			fn()
		}
		return time.Now()
	}
	return &fired
}

// atomicFenceInterleave runs an interleave of other writers at the commit point. When the run's dispatch row is free
// there (main: the pass has committed), it runs at once, before the re-arm, as in #6815. When the pass holds the row
// (the atomic pass's tail), a writer of that row would block on it inside the hook and stall the pass, so the
// interleave runs in a goroutine that waits on the row and lands after the commit; join waits for it.
type atomicFenceInterleave struct {
	fired    *atomic.Bool
	deferred bool
	done     chan error
}

func atomicFenceAtCommitPoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, service *NativeDispatchSyncRunService, fn func() error) *atomicFenceInterleave {
	t.Helper()
	interleave := &atomicFenceInterleave{done: make(chan error, 1)}
	interleave.fired = atCommitPoint(t, service, func() {
		held, err := atomicFenceDispatchRowHeld(ctx, pool)
		switch {
		case err != nil:
			interleave.done <- err
		case held:
			interleave.deferred = true
			go func() { interleave.done <- fn() }()
		default:
			interleave.done <- fn()
		}
	})
	return interleave
}

func (interleave *atomicFenceInterleave) join() error {
	if !interleave.fired.Load() {
		return errors.New("the interleave did not fire")
	}
	return <-interleave.done
}

// atomicFenceDispatchRowHeld probes the run's dispatch row with FOR UPDATE NOWAIT on another connection.
func atomicFenceDispatchRowHeld(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT 1 FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2 FOR UPDATE NOWAIT`, discoveryTestRun, outboxKindDispatchSyncRun)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
		return true, nil
	}
	return false, err
}

// atomicFenceLeaseRepair stands in for LeaseRepair.Step on R (running -> retrying) and runs its real wake.
func atomicFenceLeaseRepair(ctx context.Context, pool *pgxpool.Pool, retryAt time.Time) error {
	if _, err := pool.Exec(ctx, `
UPDATE sync_run_units SET status = 'retrying', available_at = $2, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = $1::uuid AND status = 'running'`, atomicPassRunning, retryAt); err != nil {
		return err
	}
	if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, time.Now().UTC(), syncrunrollup.DispatchWakePathLeaseRepair) {
		return errors.New("the lease-repair wake did not arm")
	}
	return nil
}

func atomicFenceWake(ctx context.Context, pool *pgxpool.Pool, at time.Time) error {
	if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, at, syncrunrollup.DispatchWakePathProviderUnit) {
		return errors.New("the wake did not arm")
	}
	return nil
}

// atomicFenceRelayDelivers stands in for the relay's claim + mark of the pending row: the newer delivery's attempt.
func atomicFenceRelayDelivers(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var attempt int64
	err := pool.QueryRow(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', attempts = attempts + 1, dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL,
    claim_route_generation = NULL, last_error = NULL, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending' AND claim_token IS NULL
RETURNING attempts`, discoveryTestRun, dispatchRouteGeneration).Scan(&attempt)
	return attempt, err
}

// atomicFenceRelayClaims stands in for the relay claim of a pending row: the next attempt, a live claim until expiresAt.
func atomicFenceRelayClaims(ctx context.Context, pool *pgxpool.Pool, expiresAt time.Time) error {
	tag, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET claim_token = gen_random_uuid()::text, claim_expires_at = $2, claim_transport = 'river',
    claim_route_generation = $3, attempts = attempts + 1, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending'`, discoveryTestRun, expiresAt, dispatchRouteGeneration)
	if err == nil && tag.RowsAffected() != 1 {
		err = fmt.Errorf("relay claim updated %d rows, want 1", tag.RowsAffected())
	}
	return err
}

type atomicFenceRow struct {
	status     string
	attempts   int64
	claimToken *string
}

func readAtomicFenceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) atomicFenceRow {
	t.Helper()
	var row atomicFenceRow
	if err := pool.QueryRow(ctx, `SELECT status, attempts, claim_token FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
		discoveryTestRun, outboxKindDispatchSyncRun).Scan(&row.status, &row.attempts, &row.claimToken); err != nil {
		t.Fatal(err)
	}
	return row
}

// TestRearmFenceOlderPassKeepsANewerDelivery (C1, #6815 D1): the pass at attempt 1 queues one unit and leaves one
// capped. At its commit point a lease repair turns R into a retry and wakes the run, and the relay delivers that wake
// at attempt 2. The old pass's re-arm must leave the row the attempt-2 delivery owns, and that delivery must run its
// pass (it claims the capped unit, whose slot R freed).
func TestRearmFenceOlderPassKeepsANewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		seedAtomicPassRun(t, ctx, pool, true, []string{atomicPassPlannedA, atomicPassPlannedB}, nil)
		retryAt := pgNow().Add(30 * time.Second)

		old := newTestDispatchService(t, pool)
		var newerAttempt int64
		interleave := atomicFenceAtCommitPoint(t, ctx, pool, old, func() error {
			if err := atomicFenceLeaseRepair(ctx, pool, retryAt); err != nil {
				return err
			}
			var err error
			newerAttempt, err = atomicFenceRelayDelivers(ctx, pool)
			return err
		})
		err := old.Dispatch(ctx, atomicPassArgs())
		if joinErr := interleave.join(); joinErr != nil {
			t.Fatalf("interleave: %v", joinErr)
		}
		t.Logf("old pass err=%v interleave deferred to after the commit=%v newer attempt=%d", err, interleave.deferred, newerAttempt)
		if err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if newerAttempt != 2 {
			t.Fatalf("precondition: newer attempt=%d, want 2", newerAttempt)
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 1 {
			t.Fatalf("precondition: old pass published %d units, want 1 (one slot free, one unit capped)", got)
		}

		row := readAtomicFenceRow(t, ctx, pool)
		if row.status != "dispatched" || row.attempts != 2 {
			t.Errorf("row status=%s attempts=%d, want dispatched/2: the attempt-1 pass's re-arm moved the row the attempt-2 delivery owns", row.status, row.attempts)
		}

		newerArgs := atomicPassArgs()
		newerArgs.DeliveryAttempt = 2
		if err := newTestDispatchService(t, pool).Dispatch(ctx, newerArgs); err != nil {
			t.Fatalf("newer delivery: %v, want nil", err)
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 2 {
			t.Errorf("published %d units after the attempt-2 delivery, want 2: the newer delivery was refused, its pass never ran", got)
		}
	})
}

// TestRearmFenceOlderPassKeepsALiveRelayClaim (C3, #6815 D1b): the pass at attempt 1 queues one unit and leaves one
// capped, so it re-arms at the 60 s countdown. At its commit point a unit end wakes the run and the relay claims that
// wake (attempt 2, claim live for 10 s). The re-arm must not clear that claim.
func TestRearmFenceOlderPassKeepsALiveRelayClaim(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		seedAtomicPassRun(t, ctx, pool, true, []string{atomicPassPlannedA, atomicPassPlannedB}, nil)

		old := newTestDispatchService(t, pool)
		var claimed atomicFenceRow
		var claimedErr error
		interleave := atomicFenceAtCommitPoint(t, ctx, pool, old, func() error {
			hookAt := time.Now().UTC()
			if err := atomicFenceWake(ctx, pool, hookAt); err != nil {
				return err
			}
			if err := atomicFenceRelayClaims(ctx, pool, hookAt.Add(10*time.Second)); err != nil {
				return err
			}
			claimedErr = pool.QueryRow(ctx, `SELECT status, attempts, claim_token FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
				discoveryTestRun, outboxKindDispatchSyncRun).Scan(&claimed.status, &claimed.attempts, &claimed.claimToken)
			return claimedErr
		})
		err := old.Dispatch(ctx, atomicPassArgs())
		if joinErr := interleave.join(); joinErr != nil {
			t.Fatalf("interleave: %v", joinErr)
		}
		t.Logf("old pass err=%v interleave deferred to after the commit=%v", err, interleave.deferred)
		if err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		if claimed.status != "pending" || claimed.attempts != 2 || claimed.claimToken == nil {
			t.Fatalf("precondition: claimed row %s/%d claim=%v, want pending/2 claim set", claimed.status, claimed.attempts, claimed.claimToken != nil)
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 1 {
			t.Fatalf("precondition: old pass published %d units, want 1 (one slot free, one unit capped)", got)
		}

		row := readAtomicFenceRow(t, ctx, pool)
		if row.status != "pending" || row.attempts != 2 || row.claimToken == nil || *row.claimToken != *claimed.claimToken {
			t.Errorf("row %s/%d claim=%v, want pending/2 with the relay's claim kept: the re-arm cleared a claim that is live now",
				row.status, row.attempts, row.claimToken != nil)
		}
	})
}
