//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The route mutation barrier (CHAOS-14523), on a real database. syncroute.Controller (Pause, Resume, ApplyCheckedIn)
// takes the route row FOR UPDATE, then `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`. The
// commit gate of every fenced kind must hold a lock that conflicts with it from before its snapshot to its COMMIT.

// routeMutationBarrierProbeSQL is the route controller's barrier, NOWAIT, so a probe never waits.
const routeMutationBarrierProbeSQL = `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE NOWAIT`

const routeBarrierTestBusyReason = "outbox_route_barrier_held_past_bound"

// probeRouteMutationBarrier takes the route controller's barrier on other, NOWAIT, and rolls it back at once. It
// returns the lock error (nil: the barrier was free).
func probeRouteMutationBarrier(t *testing.T, ctx context.Context, other *pgxpool.Pool) error {
	t.Helper()
	probe, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probe.Rollback(context.Background()) }()
	_, err = probe.Exec(ctx, routeMutationBarrierProbeSQL)
	return err
}

func isLockNotAvailableError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// TestEveryFencedCommitGateHoldsTheRouteBarrierToTheEndOfItsTransaction: for each fenced kind, the gate the pass runs
// last before COMMIT (the fence's commit snapshot; post_sync's postSyncCommitAllowed) leaves its transaction holding
// a lock that conflicts with the route controller's barrier. The barrier is free again once that transaction ends.
func TestEveryFencedCommitGateHoldsTheRouteBarrierToTheEndOfItsTransaction(t *testing.T) {
	logger := synclog.New(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	fenceGate := func(kind string, unavailable error, args func(TransportArgs) Args) func(context.Context, pgx.Tx, TransportArgs) error {
		return func(ctx context.Context, tx pgx.Tx, transport TransportArgs) error {
			fence := &transportFence{args: args(transport), kind: kind, unavailable: unavailable, logger: logger}
			snapshot, err := fence.lockCommitSnapshot(ctx, tx)
			if err == nil && !snapshot.found {
				return errors.New("the commit snapshot found no outbox row")
			}
			return err
		}
	}
	gates := []struct {
		kind, outbox string
		gate         func(context.Context, pgx.Tx, TransportArgs) error
	}{
		{outboxKindDispatchSyncRun, "00000000-0000-4000-8000-000000145231",
			fenceGate(outboxKindDispatchSyncRun, ErrDispatchSyncRunUnavailable, func(a TransportArgs) Args { return DispatchSyncRunArgs{a} })},
		{outboxKindFinalizeSyncRun, "00000000-0000-4000-8000-000000145232",
			fenceGate(outboxKindFinalizeSyncRun, ErrFinalizeSyncRunUnavailable, func(a TransportArgs) Args { return FinalizeSyncRunArgs{a} })},
		{outboxKindReferenceDiscovery, "00000000-0000-4000-8000-000000145233",
			fenceGate(outboxKindReferenceDiscovery, ErrReferenceDiscoveryUnavailable, func(a TransportArgs) Args { return ReferenceDiscoveryArgs{a} })},
		{outboxKindPostSync, "00000000-0000-4000-8000-000000145234", func(ctx context.Context, tx pgx.Tx, transport TransportArgs) error {
			allowed, err := (&NativePostSyncService{logger: logger}).postSyncCommitAllowed(ctx, tx, PostSyncArgs{transport})
			if err == nil && !allowed {
				return errors.New("the post_sync commit gate refused a current delivery")
			}
			return err
		}},
	}
	for _, gate := range gates {
		t.Run(gate.kind, func(t *testing.T) {
			withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, _ *commitLockTracer) {
				pgseed.EnsureSyncIntegration(ctx, t, other, discoveryTestOrg, discoveryTestIntegration, dispatchTestSource)
				pgseed.EnsureSyncRun(ctx, t, other, pgseed.SyncRun{ID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration})
				if got := pgseed.SyncTransportRoute(ctx, t, other, gate.kind, "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
					t.Fatalf("%s route generation = %d, want %d", gate.kind, got, dispatchRouteGeneration)
				}
				pgseed.SyncDispatchOutbox(ctx, t, other, gate.outbox, discoveryTestRun, discoveryTestOrg, gate.kind, "dispatched", "river", dispatchRouteGeneration)
				if err := probeRouteMutationBarrier(t, ctx, other); err != nil {
					t.Fatalf("barrier before the pass: %v, want free", err)
				}

				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(context.Background()) }()
				transport := TransportArgs{
					Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
					DispatchOutbox: gate.outbox, RouteGeneration: dispatchRouteGeneration,
				}
				if err := gate.gate(ctx, tx, transport); err != nil {
					t.Fatalf("commit gate: %v", err)
				}
				if err := probeRouteMutationBarrier(t, ctx, other); !isLockNotAvailableError(err) {
					t.Fatalf("barrier after the commit gate, before COMMIT: %v, want SQLSTATE 55P03 (the gate holds no lock that conflicts with SHARE ROW EXCLUSIVE)", err)
				}
				if err := tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				if err := probeRouteMutationBarrier(t, ctx, other); err != nil {
					t.Fatalf("barrier after the pass ended: %v, want free", err)
				}
				if gate.kind == outboxKindPostSync {
					assertPostSyncGateGivesUpOnAHeldBarrier(t, ctx, pool, other, transport)
				}
			})
		})
	}
}

// holdRouteMutationBarrier opens what syncroute.Controller.beginRouteMutation opens on other: the dispatch_sync_run
// route row FOR UPDATE, then SHARE ROW EXCLUSIVE on sync_dispatch_outbox. With pause, it also writes Pause's route
// change (paused, generation + 1), uncommitted. The caller commits or rolls it back before other closes.
func holdRouteMutationBarrier(t *testing.T, ctx context.Context, other *pgxpool.Pool, pause bool) pgx.Tx {
	t.Helper()
	holder, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fail := func(err error) {
		_ = holder.Rollback(context.Background())
		t.Fatal(err)
	}
	var generation int64
	if err := holder.QueryRow(ctx, `SELECT generation FROM public.sync_dispatch_transport_routes WHERE kind = $1 FOR UPDATE`,
		outboxKindDispatchSyncRun).Scan(&generation); err != nil {
		fail(err)
	}
	if _, err := holder.Exec(ctx, `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		fail(err)
	}
	if pause {
		tag, err := holder.Exec(ctx, `
UPDATE public.sync_dispatch_transport_routes
SET paused = TRUE, paused_at = now(), generation = generation + 1, updated_at = now()
WHERE kind = $1 AND generation = $2 AND paused = FALSE`, outboxKindDispatchSyncRun, generation)
		if err != nil {
			fail(err)
		}
		if tag.RowsAffected() != 1 {
			fail(errors.New("the pause update changed no route row"))
		}
	}
	return holder
}

// assertDispatchPassCommittedNothing: the run is not dispatching, its unit is still planned and no unit publish exists.
func assertDispatchPassCommittedNothing(t *testing.T, ctx context.Context, other *pgxpool.Pool, runID string) {
	t.Helper()
	if got := runStatus(t, ctx, other, runID); got == syncRunStatusDispatching {
		t.Fatal("the pass committed: the run is dispatching")
	}
	var unitStatus string
	if err := other.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE sync_run_id = $1::uuid`, runID).Scan(&unitStatus); err != nil {
		t.Fatal(err)
	}
	if unitStatus != "planned" {
		t.Fatalf("unit status = %s, want planned (the pass rolled back)", unitStatus)
	}
	var published int
	if err := other.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = $1`, jobcontract.KindSyncProviderUnit).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("unit publishes = %d, want 0", published)
	}
}

// TestDispatchGivesUpWhileARouteMutationHoldsTheBarrier: a route mutation holds its barrier for the whole pass. The
// pass never waits for it: each barrier lock fails 55P03 at once, and after the retry window the pass returns
// ErrDispatchSyncRunUnavailable naming the barrier reason, with one WARN, the counter at 1 and nothing committed.
func TestDispatchGivesUpWhileARouteMutationHoldsTheBarrier(t *testing.T) {
	reader := installBusyMeter(t)
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdRouteMutationBarrier(t, ctx, other, false)
		defer func() { _ = holder.Rollback(context.Background()) }()
		installFakeSnapshotClock(t)

		passCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := service.Dispatch(passCtx, args[0])
		if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), routeBarrierTestBusyReason) {
			t.Fatalf("pass while a route mutation holds the barrier: %v, want %v naming %s", err, ErrDispatchSyncRunUnavailable, routeBarrierTestBusyReason)
		}
		if want := len(commitSnapshotLockBackoff) + 1; tracer.rollbacks != want || tracer.busyReads() != 0 {
			t.Fatalf("savepoint rollbacks = %d, snapshot 55P03 reads = %d; want %d and 0 (the barrier lock fails, the snapshot is never sent)",
				tracer.rollbacks, tracer.busyReads(), want)
		}
		var warns []string
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=sync_dispatch_fence.commit_snapshot_busy") {
				warns = append(warns, line)
			}
		}
		if len(warns) != 1 || !strings.Contains(warns[0], "level=WARN") || !strings.Contains(warns[0], "reason="+routeBarrierTestBusyReason) ||
			!strings.Contains(warns[0], "outbox_kind="+outboxKindDispatchSyncRun) {
			t.Fatalf("give-up lines = %q, want one WARN naming the outbox kind and %s", warns, routeBarrierTestBusyReason)
		}
		if counted, want := busyCounts(t, reader), outboxKindDispatchSyncRun+"/"+routeBarrierTestBusyReason; counted[want] != 1 || len(counted) != 1 {
			t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, want)
		}
		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		assertDispatchPassCommittedNothing(t, ctx, other, args[0].RunID)
	})
}

// TestDispatchRefusesAfterARouteMutationThatHeldTheBarrierCommitsAPause: a pause (route row, barrier, paused at
// generation + 1) is open when the pass reaches its commit gate. Its barrier lock fails 55P03; the pause commits at the
// first savepoint rollback. The next read takes the lock, the snapshot sees the paused route, and the pass refuses
// with route_paused: nothing of the old generation commits.
func TestDispatchRefusesAfterARouteMutationThatHeldTheBarrierCommitsAPause(t *testing.T) {
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdRouteMutationBarrier(t, ctx, other, true)
		defer func() { _ = holder.Rollback(context.Background()) }()
		var released error
		tracer.onSavepointRollback = func() { released = holder.Commit(ctx) }
		installFakeSnapshotClock(t)

		err := service.Dispatch(ctx, args[0])
		if tracer.rollbacks == 0 {
			t.Fatal("the commit gate never met the barrier: no savepoint rollback, so the pause could not have been held off")
		}
		if released != nil {
			t.Fatalf("commit the pause: %v", released)
		}
		if err != nil {
			t.Fatalf("pass after the pause committed: %v, want nil (a refusal)", err)
		}
		refused := false
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=dispatch_sync_run.delivery_not_current") && strings.Contains(line, "reason=route_paused") {
				refused = true
			}
		}
		if !refused {
			t.Fatalf("no delivery_not_current line with reason=route_paused:\n%s", strings.Join(logs.lines(), "\n"))
		}
		assertDispatchPassCommittedNothing(t, ctx, other, args[0].RunID)
	})
}

// assertPostSyncGateGivesUpOnAHeldBarrier: post_sync polls a held route barrier within the fence's bound, like the
// other kinds. While a route mutation holds its barrier, the commit gate reads 7 times and returns
// ErrPostSyncUnavailable, its text the fixed sentinel, its cause SQLSTATE 55P03. The log is exactly two lines: the
// give-up WARN for kind post_sync naming the barrier reason, then the fan-out failure ERROR naming commit_check and
// 55P03; the counter is exactly {post_sync/barrier reason: 1}.
func assertPostSyncGateGivesUpOnAHeldBarrier(t *testing.T, ctx context.Context, pool, other *pgxpool.Pool, transport TransportArgs) {
	t.Helper()
	holder, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(ctx, `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	reader := installBusyMeter(t)
	installFakeSnapshotClock(t)
	var logs bytes.Buffer
	service := &NativePostSyncService{logger: untimedLogger(&logs)}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	allowed, gateErr := service.postSyncCommitAllowed(ctx, tx, PostSyncArgs{transport})
	if allowed || !errors.Is(gateErr, ErrPostSyncUnavailable) || gateErr.Error() != ErrPostSyncUnavailable.Error() || !isLockNotAvailableError(gateErr) {
		t.Fatalf("post_sync gate on a held barrier: allowed=%v err=%v, want ErrPostSyncUnavailable (sentinel text) caused by SQLSTATE 55P03", allowed, gateErr)
	}
	service.observeFanoutFailure(ctx, PostSyncArgs{transport}, gateErr, "", true)
	want := busyWarnLine(transport.RunID, outboxKindPostSync, routeBarrierTestBusyReason, "*fmt.wrapErrors") +
		"level=ERROR msg=post_sync_fanout.failed org_id=" + transport.OrgID + " sync_run_id=" + transport.RunID +
		" reason=commit_check error.class=postgres error.type=*fmt.wrapErrors error.code=55P03\n"
	if got := logs.String(); got != want {
		t.Fatalf("post_sync gate log =\n%q\nwant exactly\n%q", got, want)
	}
	counted := busyCounts(t, reader)
	if key := outboxKindPostSync + "/" + routeBarrierTestBusyReason; counted[key] != 1 || len(counted) != 1 {
		t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, key)
	}
}
