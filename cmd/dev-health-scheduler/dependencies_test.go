package main

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

type schedulerHandoffStepperFunc func(
	context.Context,
	time.Time,
	int,
	schedulersync.Coordinator,
) (schedulersync.HandoffResult, error)

func (function schedulerHandoffStepperFunc) HandoffDueResult(
	ctx context.Context,
	now time.Time,
	limit int,
	coordinator schedulersync.Coordinator,
) (schedulersync.HandoffResult, error) {
	return function(ctx, now, limit, coordinator)
}

type fakeSchedulerDatabase struct {
	pool        *pgxpool.Pool
	domainCalls atomic.Int64
	queueCalls  atomic.Int64
	schemaCalls atomic.Int64
	closed      atomic.Bool
}

func (database *fakeSchedulerDatabase) DomainReady(context.Context) error {
	database.domainCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) QueueReady(context.Context) error {
	database.queueCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) RiverSchemaReady(context.Context, string) error {
	database.schemaCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) DomainPool() *pgxpool.Pool { return database.pool }
func (database *fakeSchedulerDatabase) Close()                    { database.closed.Store(true) }

// stubOccurrenceStepper stands in for the pending-occurrence consumer the
// scheduler loop now requires.
type stubOccurrenceStepper struct{}

func (stubOccurrenceStepper) Reconcile(
	context.Context, time.Time, int,
) (schedulersync.OccurrenceReconcileResult, error) {
	return schedulersync.OccurrenceReconcileResult{}, nil
}

func stubOccurrenceSource(*pgxpool.Pool) (schedulersync.OccurrenceStepper, error) {
	return stubOccurrenceStepper{}, nil
}

// fakeFixedLoop records the fixed maintenance schedule lifecycle so the
// composition test can prove the scheduler process owns both schedulers.
type fakeFixedLoop struct {
	started      atomic.Int64
	stopped      atomic.Int64
	startErr     error
	readinessErr error
	coverageErr  error
}

func (*fakeFixedLoop) Name() string { return "fixed-schedule-loop" }

func (loop *fakeFixedLoop) Start(context.Context) error {
	loop.started.Add(1)
	return loop.startErr
}

func (loop *fakeFixedLoop) Shutdown(context.Context) error {
	loop.stopped.Add(1)
	return nil
}

func (loop *fakeFixedLoop) Readiness(context.Context) error { return loop.readinessErr }

func (loop *fakeFixedLoop) Coverage(context.Context) error { return loop.coverageErr }

func TestSchedulerProductionFactoryBuildsReviewedRuntime(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
	fixedLoop := &fakeFixedLoop{}
	steps := atomic.Int64{}
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
			return database, nil
		},
		newRepository: func(pool *pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			if pool != database.pool {
				t.Fatal("repository received the wrong domain pool")
			}
			return schedulerHandoffStepperFunc(func(
				context.Context,
				time.Time,
				int,
				schedulersync.Coordinator,
			) (schedulersync.HandoffResult, error) {
				steps.Add(1)
				return schedulersync.HandoffResult{}, nil
			}), nil
		},
		newCoordinator: func() schedulersync.Coordinator {
			return schedulersync.CoordinatorFunc(func(
				context.Context,
				schedulersync.HandoffTransaction,
				schedulersync.Occurrence,
			) error {
				return nil
			})
		},
		newLoop:        schedulersync.NewLoop,
		newOccurrences: stubOccurrenceSource,
		newFixedLoop: func(pool *pgxpool.Pool, _ *health.Registry) (fixedScheduleRuntime, error) {
			if pool != database.pool {
				t.Fatal("fixed schedule loop received the wrong domain pool")
			}
			return fixedLoop, nil
		},
	}
	registry := health.NewRegistry(100 * time.Millisecond)
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if component.Name() != "sync-scheduler-runtime" {
		t.Fatalf("component name = %q", component.Name())
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness = %#v", status)
	}
	if steps.Load() != 1 || database.domainCalls.Load() == 0 ||
		database.queueCalls.Load() == 0 || database.schemaCalls.Load() == 0 {
		t.Fatalf(
			"steps=%d readiness_calls=(%d,%d,%d)",
			steps.Load(),
			database.domainCalls.Load(),
			database.queueCalls.Load(),
			database.schemaCalls.Load(),
		)
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !database.closed.Load() {
		t.Fatal("scheduler runtime did not close database pools")
	}
	if fixedLoop.started.Load() != 1 || fixedLoop.stopped.Load() != 1 {
		t.Fatalf(
			"fixed schedule loop started=%d stopped=%d; the scheduler process owns both schedulers",
			fixedLoop.started.Load(), fixedLoop.stopped.Load(),
		)
	}
}

func TestSchedulerProductionFactoryClosesDatabaseOnCompositionFailure(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
	_, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(time.Second),
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return database, nil
			},
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return nil, errors.New("repository unavailable")
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: stubOccurrenceSource,
			newFixedLoop: func(*pgxpool.Pool, *health.Registry) (fixedScheduleRuntime, error) {
				return nil, errors.New("fixed schedule loop unavailable")
			},
		},
	)
	if !errors.Is(err, errSchedulerActivationUnavailable) || !database.closed.Load() {
		t.Fatalf("err=%v database_closed=%v", err, database.closed.Load())
	}
}

// Product schedule handoff and pending-occurrence reconciliation must survive a
// broken or disabled fixed maintenance scheduler. An occurrence whose marker has
// already advanced is stranded until something consumes it, so tying its fate to
// Beat's maintenance replacement would turn a maintenance outage into
// permanently unprocessed product work.
func TestSyncLoopAndOccurrenceReconcilerRunWithoutTheFixedScheduler(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
	steps := atomic.Int64{}
	registry := health.NewRegistry(100 * time.Millisecond)
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return database, nil
			},
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return schedulerHandoffStepperFunc(func(
					context.Context, time.Time, int, schedulersync.Coordinator,
				) (schedulersync.HandoffResult, error) {
					steps.Add(1)
					return schedulersync.HandoffResult{}, nil
				}), nil
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: stubOccurrenceSource,
			newFixedLoop: func(*pgxpool.Pool, *health.Registry) (fixedScheduleRuntime, error) {
				return nil, errors.New("fixed maintenance schedules are unavailable")
			},
		},
	)
	if err != nil {
		t.Fatalf("a broken fixed scheduler blocked the whole runtime: %v", err)
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v; the product loop must run without the fixed loop", err)
	}
	if steps.Load() != 1 {
		t.Fatalf("the sync loop ran %d windows without the fixed scheduler", steps.Load())
	}

	// The degradation is reported, not hidden: the fixed profile's readiness
	// names are closed while the process keeps scheduling product work.
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	if status.Ready {
		t.Fatal("readiness stayed open with the fixed scheduler unavailable")
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if !database.closed.Load() {
		t.Fatal("shutdown without a fixed loop leaked the database pools")
	}
}

// A fixed loop that constructs but fails to START must also not take the
// product loop down with it.
func TestFixedSchedulerStartFailureDoesNotStopTheSyncLoop(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
	fixedLoop := &fakeFixedLoop{startErr: errors.New("fixed loop cannot start")}
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return database, nil
			},
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return schedulerHandoffStepperFunc(func(
					context.Context, time.Time, int, schedulersync.Coordinator,
				) (schedulersync.HandoffResult, error) {
					return schedulersync.HandoffResult{}, nil
				}), nil
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: stubOccurrenceSource,
			newFixedLoop: func(*pgxpool.Pool, *health.Registry) (fixedScheduleRuntime, error) {
				return fixedLoop, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v; a fixed-loop start failure must not fail the process", err)
	}
	if fixedLoop.started.Load() != 1 {
		t.Fatalf("fixed loop start attempts = %d", fixedLoop.started.Load())
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if !database.closed.Load() {
		t.Fatal("shutdown leaked the database pools")
	}
}

// Codex round 3: the fixed-loop constructor can fail AFTER it has already
// touched the shared health registry. The registry rejects duplicate names and
// has no unregister, so a composition that registered a fallback under the same
// names on the failure path would collide and fail the whole build — stranding
// pending occurrences through a narrower path than MED-1 closed. Readiness
// ownership therefore sits with the composition root, and this proves a
// registry-touching constructor failure still leaves a startable runtime.
func TestFixedLoopConstructorFailureAfterRegistryUseStillStartsTheSyncLoop(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
	steps := atomic.Int64{}
	registry := health.NewRegistry(100 * time.Millisecond)
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return database, nil
			},
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return schedulerHandoffStepperFunc(func(
					context.Context, time.Time, int, schedulersync.Coordinator,
				) (schedulersync.HandoffResult, error) {
					steps.Add(1)
					return schedulersync.HandoffResult{}, nil
				}), nil
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: stubOccurrenceSource,
			newFixedLoop: func(_ *pgxpool.Pool, reg *health.Registry) (fixedScheduleRuntime, error) {
				// Model a constructor that gets partway through registration and
				// then fails on its final step.
				if err := reg.RegisterMetrics("fixed_scheduler", testMetricsSource{}); err != nil {
					t.Fatalf("precondition: metrics registration failed: %v", err)
				}
				return nil, errors.New("final registration step failed")
			},
		},
	)
	if err != nil {
		t.Fatalf("a registry-touching constructor failure blocked composition: %v", err)
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v; the product loop must still run", err)
	}
	if steps.Load() != 1 {
		t.Fatalf("the sync loop ran %d windows", steps.Load())
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if registry.Readiness(context.Background()).Ready {
		t.Fatal("readiness stayed open with the fixed scheduler unavailable")
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if !database.closed.Load() {
		t.Fatal("shutdown leaked the database pools")
	}
}

type testMetricsSource struct{}

func (testMetricsSource) WritePrometheus(io.Writer) error { return nil }

// The fixed readiness names must exist exactly once whether or not the runtime
// was attached, and must report unavailable while it is not.
func TestFixedScheduleGateReportsUnavailableUntilAttached(t *testing.T) {
	gate := &fixedScheduleGate{}
	if err := gate.readiness(context.Background()); !errors.Is(err, errFixedScheduleUnavailable) {
		t.Fatalf("unattached readiness = %v", err)
	}
	if err := gate.coverage(context.Background()); !errors.Is(err, errFixedScheduleUnavailable) {
		t.Fatalf("unattached coverage = %v", err)
	}
	loop := &fakeFixedLoop{}
	gate.attach(loop)
	if err := gate.readiness(context.Background()); err != nil {
		t.Fatalf("attached readiness = %v", err)
	}
	gate.detach()
	if err := gate.coverage(context.Background()); !errors.Is(err, errFixedScheduleUnavailable) {
		t.Fatalf("detached coverage = %v", err)
	}
}
