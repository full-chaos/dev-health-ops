//go:build integration

package syncdispatchruntime

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

// Route pause in the commit window (PR #6817 r1, CHAOS-14479; CHAOS-14523): a `dho workers routes pause`
// (syncroute.Controller.Pause: generation+1, paused) must not commit after the snapshot accepted the delivery and
// before the pass's COMMIT. When a commit snapshot locked only the delivery's outbox row, it could, and the pass of
// the old generation committed its unit publishes, wake and finalize arm. The hook is the same at every pin: a pgx tracer on the pass
// connection fires at the first statement after the accepted snapshot (its savepoint release excluded), that is the
// tail's first statement with the atomic pass, or COMMIT without it.

const (
	routePauseRaceRunning  = "00000000-0000-4000-8000-000000144796"
	routePauseRacePlannedA = "00000000-0000-4000-8000-000000144797"
	routePauseRacePlannedB = "00000000-0000-4000-8000-000000144798"
	routePauseRaceRetrying = "00000000-0000-4000-8000-000000144799"
	routePauseRaceApp      = "route_pause_race"

	routePauseRaceGeneration = dispatchRouteGeneration + 1
)

type routePauseRaceQueryKey struct{}

// routePauseRaceTracer arms on the connection whose commit snapshot read succeeded and fires once, synchronously, at
// that connection's next statement.
type routePauseRaceTracer struct {
	mu      sync.Mutex
	armedOn *pgx.Conn
	fired   bool
	firedAt string
	fire    func()
}

func (tracer *routePauseRaceTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	fire := tracer.armedOn == conn && !tracer.fired && !strings.HasPrefix(data.SQL, "release savepoint")
	if fire {
		tracer.fired = true
		tracer.firedAt = strings.TrimSpace(data.SQL)
	}
	tracer.mu.Unlock()
	if fire {
		tracer.fire()
	}
	return context.WithValue(ctx, routePauseRaceQueryKey{}, data.SQL)
}

func (tracer *routePauseRaceTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if sql, _ := ctx.Value(routePauseRaceQueryKey{}).(string); sql == commitSnapshotSQL && data.Err == nil {
		tracer.mu.Lock()
		if !tracer.fired {
			tracer.armedOn = conn
		}
		tracer.mu.Unlock()
	}
}

type routePauseRaceShape struct {
	name       string
	cap        string
	running    bool
	planned    []string
	retryingIn time.Duration
}

// Every pass shape of the r1 finding. "Zero queued, waiting on in-flight units" commits no row; only the pause wait
// shows its window.
var routePauseRaceShapes = []routePauseRaceShape{
	{name: "queued_and_capped", cap: "2", running: true, planned: []string{routePauseRacePlannedA, routePauseRacePlannedB}},
	{name: "queued_no_wake", cap: "2", planned: []string{routePauseRacePlannedA}},
	{name: "nothing_queued_capped", cap: "1", running: true, planned: []string{routePauseRacePlannedA}},
	{name: "nothing_queued_deferred", cap: "1", retryingIn: 30 * time.Minute},
	{name: "nothing_left_finalize", cap: "1"},
	{name: "nothing_queued_waiting_inflight", cap: "1", running: true},
}

func (shape routePauseRaceShape) seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", shape.cap)
	seedDispatchRoute(t, ctx, pool)
	markReferenceDiscoverySucceeded(t, ctx, pool)
	// The checked-in policy names rollback_route none for dispatch_sync_run and Pause validates the row against it; the
	// route table's trigger wants a generation increase for that change, so the delivery is at routePauseRaceGeneration.
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_transport_routes SET rollback_transport = 'none', generation = $2 WHERE kind = $1`,
		outboxKindDispatchSyncRun, routePauseRaceGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1, dispatched_route_generation = $2 WHERE id = $1`,
		dispatchServiceTestOutbox, routePauseRaceGeneration); err != nil {
		t.Fatal(err)
	}
	now := pgNow()
	if shape.running {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,lease_owner,lease_expires_at,last_heartbeat_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'running',$5,'live-worker',$6,$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',1,now())`,
			routePauseRaceRunning, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range shape.planned {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'planned',$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',0,now())`,
			id, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now); err != nil {
			t.Fatal(err)
		}
	}
	if shape.retryingIn > 0 {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,available_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'retrying',$5,$6,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',1,now())`,
			routePauseRaceRetrying, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now, now.Add(shape.retryingIn)); err != nil {
			t.Fatal(err)
		}
	}
}

// routePauseRaceController is the controller `dho workers routes pause` builds (internal/workersctl), on its own pool.
func routePauseRaceController(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*syncroute.Controller, *pgxpool.Pool) {
	t.Helper()
	config := pool.Config()
	params := map[string]string{}
	for key, value := range config.ConnConfig.RuntimeParams {
		params[key] = value
	}
	params["application_name"] = routePauseRaceApp
	config.ConnConfig.RuntimeParams = params
	pausePool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pausePool.Close)
	registry, err := syncdispatchcontract.Load(filepath.Join("..", "..", "contracts", "sync-dispatch", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := syncroute.NewCapabilities(RouteCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	controller, err := syncroute.NewController(pausePool, registry, capabilities)
	if err != nil {
		t.Fatal(err)
	}
	return controller, pausePool
}

func routePauseRaceCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestRoutePauseInTheCommitWindowCommitsNothingForTheOldGeneration: a real route pause starts between the accepted
// commit snapshot and the pass's next statement. WANT per shape: the pause waits on a lock the pass holds (CHAOS-14523:
// the route mutation barrier conflicts with the pass), so it commits after the pass. A pause that commits inside the
// window is a failure, and then the pass of the delivery generation must still commit no publish, no wake and no
// finalize arm.
func TestRoutePauseInTheCommitWindowCommitsNothingForTheOldGeneration(t *testing.T) {
	for _, shape := range routePauseRaceShapes {
		t.Run(shape.name, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				shape.seed(t, ctx, pool)
				planned := routePauseRaceCount(t, ctx, pool, `SELECT count(*) FROM sync_run_units WHERE sync_run_id = $1 AND status = $2`, discoveryTestRun, syncRunUnitStatusPlanned)
				controller, _ := routePauseRaceController(t, ctx, pool)

				var pauseErr error
				var pausedGeneration int64
				pauseDone := make(chan struct{})
				var pauseInWindow, pauseWaited bool
				tracer := &routePauseRaceTracer{}
				tracer.fire = func() {
					go func() {
						defer close(pauseDone)
						var state syncroute.RouteState
						state, pauseErr = controller.Pause(ctx, outboxKindDispatchSyncRun)
						pausedGeneration = state.Generation
					}()
					deadline := time.Now().Add(10 * time.Second)
					for time.Now().Before(deadline) {
						select {
						case <-pauseDone:
							pauseInWindow = true
							return
						case <-time.After(10 * time.Millisecond):
						}
						if routePauseRaceCount(t, ctx, pool, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'`, routePauseRaceApp) > 0 {
							pauseWaited = true
							return
						}
					}
				}
				config := pool.Config()
				config.ConnConfig.Tracer = tracer
				passPool, err := pgxpool.NewWithConfig(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				defer passPool.Close()

				dispatchErr := newTestDispatchService(t, passPool).Dispatch(ctx, routePauseRaceArgs())
				if !tracer.fired {
					t.Fatalf("the hook did not fire: no accepted commit snapshot (dispatch err=%v)", dispatchErr)
				}
				select {
				case <-pauseDone:
				case <-time.After(30 * time.Second):
					t.Fatal("the pause did not end")
				}
				if pauseErr != nil {
					t.Fatalf("pause: %v", pauseErr)
				}

				published := routePauseRaceCount(t, ctx, pool, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = $1`, jobcontract.KindSyncProviderUnit)
				plannedAfter := routePauseRaceCount(t, ctx, pool, `SELECT count(*) FROM sync_run_units WHERE sync_run_id = $1 AND status = $2`, discoveryTestRun, syncRunUnitStatusPlanned)
				finalize := routePauseRaceCount(t, ctx, pool, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`, discoveryTestRun, outboxKindFinalizeSyncRun)
				var status string
				var attempts int64
				if err := pool.QueryRow(ctx, `SELECT status, attempts FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
					discoveryTestRun, outboxKindDispatchSyncRun).Scan(&status, &attempts); err != nil {
					t.Fatal(err)
				}
				var routeGeneration int64
				var routePaused bool
				if err := pool.QueryRow(ctx, `SELECT generation, paused FROM sync_dispatch_transport_routes WHERE kind = $1`, outboxKindDispatchSyncRun).
					Scan(&routeGeneration, &routePaused); err != nil {
					t.Fatal(err)
				}
				t.Logf("shape=%s hook_at=%q pause_in_window=%v pause_waited=%v pause_generation=%d route=%d/paused=%v dispatch_err=%v published=%d planned=%d->%d row=%s/%d finalize_rows=%d",
					shape.name, tracer.firedAt, pauseInWindow, pauseWaited, pausedGeneration, routeGeneration, routePaused, dispatchErr,
					published, planned, plannedAfter, status, attempts, finalize)
				if !routePaused || routeGeneration != routePauseRaceGeneration+1 {
					t.Fatalf("precondition: route generation=%d paused=%v, want %d paused", routeGeneration, routePaused, routePauseRaceGeneration+1)
				}
				if !pauseWaited || pauseInWindow {
					t.Errorf("window: the pause committed between the accepted commit snapshot and the pass's COMMIT (waited=%v, in_window=%v); want it to wait on a lock the pass holds",
						pauseWaited, pauseInWindow)
				}
				if !pauseInWindow {
					return
				}
				if published != 0 || plannedAfter != planned {
					t.Errorf("publish: the generation-%d pass committed %d unit publishes (planned %d->%d) after the route paused at generation %d",
						routePauseRaceGeneration, published, planned, plannedAfter, routeGeneration)
				}
				if status != "dispatched" || attempts != 1 {
					t.Errorf("wake: the generation-%d pass left the row %s/%d after the route paused, want dispatched/1 (no old-generation wake)",
						routePauseRaceGeneration, status, attempts)
				}
				if finalize != 0 {
					t.Errorf("finalize: the generation-%d pass armed finalize (%d rows) after the route paused", routePauseRaceGeneration, finalize)
				}
			})
		})
	}
}

func routePauseRaceArgs() DispatchSyncRunArgs {
	args := dispatchTestArgs()
	args.DeliveryAttempt = 1
	args.TransportArgs.RouteGeneration = routePauseRaceGeneration
	return args
}
