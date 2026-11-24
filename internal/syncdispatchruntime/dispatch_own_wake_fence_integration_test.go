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

// Own-wake fence cells: the wake a dispatch pass defers for its own run after its commit (path dispatch_pass) must
// not move a row a newer delivery owns, and every other wake writer keeps waking a dispatched row.

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
// bucket (cap 2) and terminalizes them, so it defers its own wake; the valid sibling stays planned. After its commit
// and before that wake, a unit end wakes the run and the relay delivers the wake at attempt 2. The old pass's own wake
// must leave the row the attempt-2 delivery owns, and that delivery must run its pass.
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
		if err := old.Dispatch(ctx, atomicPassArgs()); err != nil {
			t.Fatalf("old pass: %v, want nil", err)
		}
		t.Logf("old pass: end line seen=%v row at the end line=%s/%d interleave err=%v newer attempt=%d",
			fired.Load(), afterCommit.status, afterCommit.attempts, interleaveErr, newerAttempt)
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

// TestOtherWakeCallersStillWakeADispatchedRow: one case per caller class of the unfenced wake. providersync's unit end
// (PostgresRepository.wakeDispatch) and the reconciler writers (wakeDispatchAfterCommit: lease repair, route
// unavailable, unreclaimable sweep) call syncrunrollup.WakeDispatch with their path; the hold-back wake is
// wakeHeldBackRuns. Each must still move a row dispatched at attempt 5 to pending, due by its instant, attempts kept.
func TestOtherWakeCallersStillWakeADispatchedRow(t *testing.T) {
	wakeWith := func(path string) func(ctx context.Context, pool *pgxpool.Pool, at time.Time) bool {
		return func(ctx context.Context, pool *pgxpool.Pool, at time.Time) bool {
			return syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, at, path)
		}
	}
	for _, caller := range []struct {
		name string
		wake func(ctx context.Context, pool *pgxpool.Pool, at time.Time) bool
	}{
		{"providersync_unit_end", wakeWith(syncrunrollup.DispatchWakePathProviderUnit)},
		{"reconciler_lease_repair", wakeWith(syncrunrollup.DispatchWakePathLeaseRepair)},
		{"reconciler_route_unavailable", wakeWith(syncrunrollup.DispatchWakePathRouteUnavailable)},
		{"reconciler_unreclaimable_sweep", wakeWith(syncrunrollup.DispatchWakePathUnreclaimableSweep)},
		{"hold_back", func(ctx context.Context, pool *pgxpool.Pool, at time.Time) bool {
			return wakeHeldBackRuns(ctx, pool, []string{discoveryTestRun}, at) == 1
		}},
	} {
		t.Run(caller.name, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				seedAtomicPassRun(t, ctx, pool, false, []string{atomicPassPlannedA}, nil)
				if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 5 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
					t.Fatal(err)
				}
				at := pgNow()
				if !caller.wake(ctx, pool, at) {
					t.Fatalf("%s: the wake reported nothing armed, want armed", caller.name)
				}
				if got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun); got.After(at) {
					t.Fatalf("%s: available_at=%s, want <= the wake instant %s", caller.name, got, at)
				}
				row := readAtomicFenceRow(t, ctx, pool)
				if row.status != "pending" || row.attempts != 5 {
					t.Fatalf("%s: row %s/%d, want pending/5: this caller no longer wakes a dispatched row", caller.name, row.status, row.attempts)
				}
			})
		})
	}
}
