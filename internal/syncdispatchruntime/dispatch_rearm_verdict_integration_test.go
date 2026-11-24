//go:build integration

package syncdispatchruntime

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// Verdict cells on main for the two re-arm cases the atomic pass did not move: the pass's own deferred wake
// (syncrunrollup.WakeDispatch, path dispatch_pass, after the commit) and a relay claim that lands before the commit
// snapshot. Each WANT fails when an older pass writes a row a newer delivery owns.

// onPassEndLine is a slog handler that runs fn once, at the first pass end line (after the fenced commit, before the
// pass's deferred wakes run).
type onPassEndLine struct {
	fired *atomic.Bool
	fn    func()
}

func (handler onPassEndLine) Enabled(context.Context, slog.Level) bool { return true }
func (handler onPassEndLine) Handle(_ context.Context, record slog.Record) error {
	switch record.Message {
	case "dispatch_sync_run.noop", "dispatch_sync_run.deferred", "dispatch_sync_run.dispatched", "dispatch_sync_run.waiting_inflight":
		if handler.fired.CompareAndSwap(false, true) {
			handler.fn()
		}
	}
	return nil
}
func (handler onPassEndLine) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler onPassEndLine) WithGroup(string) slog.Handler      { return handler }

// TestRearmVerdictOlderPassOwnWakeKeepsANewerDelivery: the pass at attempt 1 claims two unroutable units of the
// bucket (cap 2) and terminalizes them, so it defers WakeDispatch for its own run; the valid sibling stays planned.
// After its commit and before that wake, a unit end wakes the run and the relay delivers the wake at attempt 2. The
// old pass's own wake must leave the row the attempt-2 delivery owns, and that delivery must run its pass.
func TestRearmVerdictOlderPassOwnWakeKeepsANewerDelivery(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}
		for _, unit := range []struct{ id, dataset string }{
			{"00000000-0000-4000-8000-0000001447d0", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447d1", "unroutable-dataset"},
			{"00000000-0000-4000-8000-0000001447d2", "commits"},
		} {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$4,$5,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
				unit.id, discoveryTestOrg, discoveryTestRun, unit.dataset, dispatchTestSource); err != nil {
				t.Fatal(err)
			}
		}

		old := newTestDispatchService(t, pool)
		var fired atomic.Bool
		var afterCommit atomicFenceRow
		var newerAttempt int64
		var interleaveErr error
		old.logger = synclog.New(slog.New(onPassEndLine{fired: &fired, fn: func() {
			afterCommit = readAtomicFenceRow(t, ctx, pool)
			if interleaveErr = atomicFenceWake(ctx, pool, time.Now().UTC()); interleaveErr != nil {
				return
			}
			newerAttempt, interleaveErr = atomicFenceRelayDelivers(ctx, pool)
		}}))
		armedBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathDispatchPass, syncrunrollup.DispatchWakeOutcomeArmed)
		if err := old.Dispatch(ctx, atomicPassArgs()); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		armed := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathDispatchPass, syncrunrollup.DispatchWakeOutcomeArmed) - armedBefore
		t.Logf("old pass: end line seen=%v row at the end line=%s/%d interleave err=%v newer attempt=%d dispatch_pass wakes armed=%d",
			fired.Load(), afterCommit.status, afterCommit.attempts, interleaveErr, newerAttempt, armed)
		if !fired.Load() || interleaveErr != nil || newerAttempt != 2 {
			t.Fatalf("precondition: end line seen=%v interleave err=%v newer attempt=%d, want true/nil/2", fired.Load(), interleaveErr, newerAttempt)
		}
		if failed := unitIDsInStatus(t, ctx, pool, "failed"); len(failed) != 2 {
			t.Fatalf("precondition: %d units terminalized in the old pass, want the 2 unroutable ones (%v)", len(failed), failed)
		}
		if planned := unitIDsInStatus(t, ctx, pool, "planned"); len(planned) != 1 {
			t.Fatalf("precondition: %d planned units, want the 1 sibling the cap held back (%v)", len(planned), planned)
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 0 {
			t.Fatalf("precondition: old pass published %d units, want 0", got)
		}

		row := readAtomicFenceRow(t, ctx, pool)
		t.Logf("row after the old pass's own wake: %s/%d", row.status, row.attempts)
		if row.status != "dispatched" || row.attempts != 2 {
			t.Errorf("row status=%s attempts=%d, want dispatched/2: the attempt-1 pass's own deferred wake moved the row the attempt-2 delivery owns", row.status, row.attempts)
		}

		newerArgs := atomicPassArgs()
		newerArgs.DeliveryAttempt = 2
		if err := newTestDispatchService(t, pool).Dispatch(ctx, newerArgs); err != nil {
			t.Fatalf("newer delivery: %v, want nil", err)
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 1 {
			t.Errorf("published %d units after the attempt-2 delivery, want 1: the newer delivery was refused, its pass never ran", got)
		}
	})
}

// TestRearmVerdictRelayClaimBeforeTheCommitSnapshotIsKept (D1b): the pass at attempt 1 queues one unit and leaves
// one capped, so its re-arm targets armAt + the 60 s countdown. Before its commit snapshot a unit end wakes the run and
// the relay claims that wake (attempt 2, claim live for 10 s). The claim is live now; only its expiry against the
// re-arm's far target makes it look dead. The relay's claim must be kept.
func TestRearmVerdictRelayClaimBeforeTheCommitSnapshotIsKept(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		seedAtomicPassRun(t, ctx, pool, true, []string{atomicPassPlannedA, atomicPassPlannedB}, nil)

		old := newTestDispatchService(t, pool)
		var fired atomic.Bool
		var held bool
		var claimed atomicFenceRow
		var interleaveErr error
		saved := commitSnapshotClock
		t.Cleanup(func() { commitSnapshotClock = saved })
		commitSnapshotClock.now = func() time.Time {
			if fired.CompareAndSwap(false, true) {
				if held, interleaveErr = atomicFenceDispatchRowHeld(ctx, pool); interleaveErr != nil || held {
					return time.Now()
				}
				hookAt := time.Now().UTC()
				if interleaveErr = atomicFenceWake(ctx, pool, hookAt); interleaveErr != nil {
					return time.Now()
				}
				if interleaveErr = atomicFenceRelayClaims(ctx, pool, hookAt.Add(10*time.Second)); interleaveErr != nil {
					return time.Now()
				}
				interleaveErr = pool.QueryRow(ctx, `SELECT status, attempts, claim_token FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
					discoveryTestRun, outboxKindDispatchSyncRun).Scan(&claimed.status, &claimed.attempts, &claimed.claimToken)
			}
			return time.Now()
		}
		err := old.Dispatch(ctx, atomicPassArgs())
		published := atomicPassPublishes(t, ctx, pool)
		t.Logf("old pass err=%v hook fired=%v row held at the hook=%v interleave err=%v published=%d", err, fired.Load(), held, interleaveErr, published)
		if !fired.Load() || held || interleaveErr != nil {
			t.Fatalf("precondition: hook fired=%v row held=%v interleave err=%v, want true/false/nil", fired.Load(), held, interleaveErr)
		}
		if claimed.status != "pending" || claimed.attempts != 2 || claimed.claimToken == nil {
			t.Fatalf("precondition: claimed row %s/%d claim=%v, want pending/2 claim set", claimed.status, claimed.attempts, claimed.claimToken != nil)
		}
		if err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}

		row := readAtomicFenceRow(t, ctx, pool)
		t.Logf("row after the old pass: %s/%d claim=%v", row.status, row.attempts, row.claimToken != nil)
		if row.status != "pending" || row.attempts != 2 || row.claimToken == nil || *row.claimToken != *claimed.claimToken {
			t.Errorf("row %s/%d claim=%v, want pending/2 with the relay's claim kept: the old pass cleared a claim that is live now",
				row.status, row.attempts, row.claimToken != nil)
		}
		if published != 0 {
			t.Errorf("published=%d, want 0: the attempt-1 pass committed after the relay claimed attempt 2", published)
		}
	})
}
