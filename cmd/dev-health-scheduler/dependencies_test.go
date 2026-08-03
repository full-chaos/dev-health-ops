package main

import (
	"context"
	"errors"
	"sync"
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
	pool             *pgxpool.Pool
	coordinatorPool  *pgxpool.Pool
	domainCalls      atomic.Int64
	queueCalls       atomic.Int64
	coordinatorCalls atomic.Int64
	schemaCalls      atomic.Int64
	closed           atomic.Bool
}

func (database *fakeSchedulerDatabase) DomainReady(context.Context) error {
	database.domainCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) QueueReady(context.Context) error {
	database.queueCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) CoordinatorReady(context.Context) error {
	database.coordinatorCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) RiverSchemaReady(context.Context, string) error {
	database.schemaCalls.Add(1)
	return nil
}

func (database *fakeSchedulerDatabase) DomainPool() *pgxpool.Pool { return database.pool }

// CoordinatorPool mirrors postgres.RuntimePools.CoordinatorPool's fail-closed
// contract: a fake with no coordinatorPool configured reports ErrUnavailable
// rather than silently handing back the domain pool.
func (database *fakeSchedulerDatabase) CoordinatorPool() (*pgxpool.Pool, error) {
	if database.coordinatorPool == nil {
		return nil, errSchedulerActivationUnavailable
	}
	return database.coordinatorPool, nil
}

func (database *fakeSchedulerDatabase) Close() { database.closed.Store(true) }

// stubOccurrenceStepper stands in for the pending-occurrence consumer the
// scheduler loop now requires.
type stubOccurrenceStepper struct{}

func (stubOccurrenceStepper) Reconcile(
	context.Context, time.Time, int,
) (schedulersync.OccurrenceReconcileResult, error) {
	return schedulersync.OccurrenceReconcileResult{}, nil
}

func stubOccurrenceSource(*pgxpool.Pool, *pgxpool.Pool) (schedulersync.OccurrenceStepper, error) {
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

// TestProductionSchedulerRuntimeSourcesRepositoryFollowsSchedulerOwnership
// proves schedulerOwnership (main.go) is the actual source of truth for which
// repository the production composition builds, not a value that is merely
// validated. The default-ownership branch is exercised behaviorally (its
// HandoffDueResult must refuse before ever touching the pool, so a zero-value
// pool is safe to use here); the transferred branch is checked structurally,
// since exercising HandoffDueResult against an unconnected pool would reach
// past the ownership check into pool.BeginTx.
func TestProductionSchedulerRuntimeSourcesRepositoryFollowsSchedulerOwnership(t *testing.T) {
	original := schedulerOwnership
	defer func() { schedulerOwnership = original }()
	pool := &pgxpool.Pool{}

	schedulerOwnership = schedulersync.DefaultOwnershipPolicy()
	defaultStepper, err := productionSchedulerRuntimeSources.newRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := defaultStepper.HandoffDueResult(
		context.Background(), time.Now(), 1, schedulersync.NewOccurrenceCoordinator(),
	); !errors.Is(err, schedulersync.ErrSchedulerMutationDisabled) {
		t.Fatalf("default ownership HandoffDueResult() = %v, want ErrSchedulerMutationDisabled", err)
	}

	schedulerOwnership = schedulersync.TransferScheduleMarkerOwnershipToGo()
	mutationStepper, err := productionSchedulerRuntimeSources.newRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := mutationStepper.(*schedulersync.Repository); !ok || mutationStepper == nil {
		t.Fatalf("transferred ownership repository = %#v, want a non-nil *schedulersync.Repository", mutationStepper)
	}
}

func TestSchedulerProductionFactoryBuildsReviewedRuntime(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
	fixedLoop := &fakeFixedLoop{}
	steps := atomic.Int64{}
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
			return database, nil
		},
		newRepository: func(pool *pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			// CHAOS-3114: the handoff repository must run on the coordinator
			// pool, not the domain pool -- sync_configurations and
			// scheduled_jobs are coordinator-exclusive and the handoff's
			// `FOR UPDATE OF config, job` requires UPDATE on both.
			if pool != database.coordinatorPool {
				t.Fatal("repository received the wrong pool; want the coordinator pool")
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
		newLoop: schedulersync.NewLoop,
		newOccurrences: func(pool, domainPool *pgxpool.Pool) (schedulersync.OccurrenceStepper, error) {
			// CHAOS-3114: scheduled_sync_occurrences is coordinator-exclusive,
			// so the reconciler must also run on the coordinator pool.
			if pool != database.coordinatorPool {
				t.Fatal("occurrence reconciler received the wrong pool; want the coordinator pool")
			}
			if domainPool != database.pool {
				t.Fatal("materializer received the wrong pool; want the domain pool")
			}
			return stubOccurrenceSource(pool, domainPool)
		},
		newFixedLoop: func(pool *pgxpool.Pool, _ *health.Registry) (fixedScheduleRuntime, error) {
			// CHAOS-3114 (second half): the fixed engine runs on the
			// coordinator pool as well. runOccurrence commits the
			// coordinator-exclusive occurrence ledger together with the
			// producers' domain rows in one transaction, and coordinatorPosture
			// is what covers that whole statement set -- handing this call site
			// the domain pool fails the ledger claim with SQLSTATE 42501.
			if pool != database.coordinatorPool {
				t.Fatal("fixed schedule loop received the wrong pool; want the coordinator pool")
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
		database.queueCalls.Load() == 0 || database.coordinatorCalls.Load() == 0 ||
		database.schemaCalls.Load() == 0 {
		t.Fatalf(
			"steps=%d readiness_calls=(%d,%d,%d,%d)",
			steps.Load(),
			database.domainCalls.Load(),
			database.queueCalls.Load(),
			database.coordinatorCalls.Load(),
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
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
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
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
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
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
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

// The original defect: the fixed-loop constructor claimed BOTH readiness names
// and then failed on its final registration step, so the composition's failure
// path could not register fallbacks under those names and the whole build
// failed — taking the sync loop and its occurrence reconciler down with it.
//
// This fake reproduces that ordering exactly. Under the old code its readiness
// registrations would have succeeded and the fallback would then have
// collided; under the current code the composition root already owns both
// names, so the attempts fail here instead. Either way the constructor returns
// an error, and the property under test is that composition survives it.
func TestFixedLoopConstructorFailureAfterClaimingReadinessNamesStillStartsTheSyncLoop(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
	steps := atomic.Int64{}
	registry := health.NewRegistry(100 * time.Millisecond)
	var readinessAttempts, readinessRejected int

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
				// Step 1: claim both readiness names, exactly as the old
				// constructor did before its metrics registration.
				for _, name := range []string{"fixed_scheduler_loop", "fixed_schedule_coverage"} {
					readinessAttempts++
					if err := reg.RegisterRequired(name, func(context.Context) error {
						return nil
					}); err != nil {
						readinessRejected++
					}
				}
				// Step 2: fail on the final registration step.
				return nil, errors.New("final registration step failed")
			},
		},
	)
	if err != nil {
		t.Fatalf("a constructor that claimed the readiness names blocked composition: %v", err)
	}
	if readinessAttempts != 2 {
		t.Fatalf("the fake did not reproduce the ordering: %d readiness attempts", readinessAttempts)
	}
	// Proves single ownership: the composition root already holds both names, so
	// no constructor can claim them and no fallback can collide.
	if readinessRejected != 2 {
		t.Fatalf("constructor claimed %d readiness names; the composition root must own both",
			2-readinessRejected)
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

// blockingFixedLoop parks inside its readiness check until released, so a poll
// can be held in flight across a detach.
type blockingFixedLoop struct {
	fakeFixedLoop
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (loop *blockingFixedLoop) Readiness(context.Context) error {
	loop.once.Do(func() { close(loop.entered) })
	<-loop.release
	return nil
}

// A readiness poll must never report healthy at a point in time after detach
// has completed. Copying the runtime and releasing the lock before invoking the
// check allowed exactly that: the poll observed a healthy runtime, shutdown
// detached, and the poll then answered healthy for a scheduler that was already
// gone.
func TestDetachDoesNotPermitAHealthyAnswerAfterItReturns(t *testing.T) {
	gate := &fixedScheduleGate{}
	loop := &blockingFixedLoop{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	gate.attach(loop)

	pollResult := make(chan error, 1)
	go func() { pollResult <- gate.readiness(context.Background()) }()
	<-loop.entered

	detachReturned := make(chan struct{})
	go func() {
		gate.detach()
		close(detachReturned)
	}()

	// Detach must not complete while a check is in flight; otherwise the poll
	// could answer healthy afterwards.
	select {
	case <-detachReturned:
		t.Fatal("detach returned while a readiness check was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(loop.release)
	if err := <-pollResult; err != nil {
		t.Fatalf("the in-flight poll started while attached and should have answered healthy: %v", err)
	}
	<-detachReturned

	// Every result observable after detach returns must be unavailable.
	if err := gate.readiness(context.Background()); !errors.Is(err, errFixedScheduleUnavailable) {
		t.Fatalf("readiness after detach = %v", err)
	}
	if gate.attached() {
		t.Fatal("gate still reports a runtime after detach")
	}
}

// Hammer the gate concurrently: a poll that BEGINS after detach has returned
// must never report healthy. Run under the race detector.
//
// The "begins after" framing matters. A poll that started while the runtime was
// attached is entitled to answer healthy even if detach completes before the
// caller inspects the result — that answer is correct for the instant it was
// taken. The defect being guarded is narrower: a poll starting from a detached
// gate must never find a stale runtime to ask.
func TestConcurrentPollsNeverSeeHealthyAfterDetachCompletes(t *testing.T) {
	gate := &fixedScheduleGate{}
	gate.attach(&fakeFixedLoop{})

	var detachCompleted atomic.Bool
	var staleHealthy atomic.Int64
	stop := make(chan struct{})
	var pollers sync.WaitGroup

	for worker := 0; worker < 8; worker++ {
		pollers.Add(1)
		go func() {
			defer pollers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// The flag is sampled immediately before EACH call, never once
				// for both. Sampling after a call would flag one that
				// legitimately began while attached; sampling once for two
				// calls would evaluate the second against the first's stale
				// observation, so a coverage call that genuinely began from a
				// detached gate could go unchecked.
				if detachCompleted.Load() && gate.readiness(context.Background()) == nil {
					staleHealthy.Add(1)
				}
				if detachCompleted.Load() && gate.coverage(context.Background()) == nil {
					staleHealthy.Add(1)
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	gate.detach()
	detachCompleted.Store(true)
	time.Sleep(20 * time.Millisecond)
	close(stop)
	pollers.Wait()

	if staleHealthy.Load() != 0 {
		t.Fatalf("%d polls that began after detach completed still reported healthy",
			staleHealthy.Load())
	}
}

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
	gate.attach(&fakeFixedLoop{})
	if err := gate.readiness(context.Background()); err != nil {
		t.Fatalf("attached readiness = %v", err)
	}
	gate.detach()
	if err := gate.coverage(context.Background()); !errors.Is(err, errFixedScheduleUnavailable) {
		t.Fatalf("detached coverage = %v", err)
	}
}
