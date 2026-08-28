package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/processreadiness"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/jackc/pgx/v5/pgxpool"
)

type schedulerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	CoordinatorReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	DomainPool() *pgxpool.Pool
	CoordinatorPool() (*pgxpool.Pool, error)
	Close()
}

type postgresSchedulerDatabase struct {
	pools           *postgres.RuntimePools
	domainRole      string
	queueRole       string
	coordinatorRole string
	riverSchema     string
}

// openSchedulerDatabase opts into the coordinator boundary via
// WithCoordinator: CHAOS-3114 repoints the sync repository, the occurrence
// reconciler AND the fixed maintenance engine onto the coordinator pool.
// sync_configurations, scheduled_jobs, scheduled_sync_occurrences and
// fixed_schedule_occurrences are coordinator-exclusive per
// internal/storage/postgres/domain_authorization.go, and
// internal/scheduler/sync/transaction.go's `FOR UPDATE OF config, job
// SKIP LOCKED` requires UPDATE on those rows even though it writes nothing
// else. deploy/go-workers/deployment.json already budgets
// coordinator_max_connections: 2 for the "scheduler" process, and both loops
// are single-transaction-at-a-time (the sync loop hands off then reconciles
// sequentially; the fixed engine runs one occurrence transaction at a time),
// so peak concurrent demand is one connection per loop -- exactly the
// budgeted 2, with no change to the connection footprint the deployment
// contract already accounts for.
func openSchedulerDatabase(ctx context.Context, cfg config.Config) (schedulerDatabase, error) {
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg).WithCoordinator()
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresSchedulerDatabase{
		pools:           pools,
		domainRole:      runtimeConfig.DomainRole,
		queueRole:       runtimeConfig.QueueRole,
		coordinatorRole: runtimeConfig.CoordinatorRole,
		riverSchema:     runtimeConfig.RiverSchema,
	}, nil
}

func (database *postgresSchedulerDatabase) DomainReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Domain == nil {
		return errSchedulerActivationUnavailable
	}
	return postgres.CheckDomainAuthorization(
		ctx,
		database.pools.Domain,
		database.domainRole,
		database.riverSchema,
	)
}

func (database *postgresSchedulerDatabase) QueueReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errSchedulerActivationUnavailable
	}
	return postgres.CheckQueueAuthorization(
		ctx,
		database.pools.QueueControl,
		database.queueRole,
		database.riverSchema,
	)
}

// CoordinatorReady proves the coordinator login holds exactly
// coordinatorPosture. It is a separate readiness gate from DomainReady on
// purpose: cross-role attribution is distributed, so only the coordinator's
// own check can catch a privilege wrongly granted to the coordinator role.
func (database *postgresSchedulerDatabase) CoordinatorReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Coordinator == nil {
		return errSchedulerActivationUnavailable
	}
	return postgres.CheckCoordinatorAuthorization(
		ctx,
		database.pools.Coordinator,
		database.coordinatorRole,
		database.riverSchema,
	)
}

func (database *postgresSchedulerDatabase) RiverSchemaReady(
	ctx context.Context,
	schema string,
) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errSchedulerActivationUnavailable
	}
	_, err := riverstore.CheckSchema(ctx, database.pools.QueueControl, schema, nil)
	return err
}

// DomainPool is the native scheduled-sync materializer's persistence pool.
// Policy locks and coordinator ledgers stay on CoordinatorPool; sync_runs,
// sync_run_units, and FK-dependent provider inventory repair commit together
// on the domain transaction.
func (database *postgresSchedulerDatabase) DomainPool() *pgxpool.Pool {
	if database == nil || database.pools == nil {
		return nil
	}
	return database.pools.Domain
}

// CoordinatorPool never falls back to the domain pool. RuntimePools.
// CoordinatorPool fails closed with postgres.ErrUnavailable when the
// coordinator pool was never opened, which is the intended behaviour: a
// coordinator call site silently running on the domain role is exactly the
// CHAOS-3113/CHAOS-3114 defect this split removes.
func (database *postgresSchedulerDatabase) CoordinatorPool() (*pgxpool.Pool, error) {
	if database == nil || database.pools == nil {
		return nil, errSchedulerActivationUnavailable
	}
	return database.pools.CoordinatorPool()
}

func (database *postgresSchedulerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

// fixedScheduleRuntime is the fixed maintenance scheduler as this process uses
// it: a lifecycle component that also supplies its own readiness checks. The
// checks are supplied rather than self-registered so exactly one place owns the
// readiness names.
//
// Readiness and Coverage are invoked while the gate holds a read lock, so both
// must be bounded and must never call back into the gate. Neither performs I/O
// today: Readiness reads an atomic, and Coverage compares in-memory schedule
// declarations.
type fixedScheduleRuntime interface {
	lifecycle.Component
	Readiness(context.Context) error
	Coverage(context.Context) error
}

var errFixedScheduleUnavailable = errors.New("fixed maintenance scheduler is unavailable")

// fixedScheduleGate owns the two fixed-scheduler readiness names for the whole
// process lifetime.
//
// It is registered unconditionally, before anything fallible runs, and has the
// runtime attached only once construction and startup both succeed. That
// ordering is the whole point: the health registry rejects duplicate names and
// offers no unregister, so any scheme that registers on success and registers a
// fallback on failure can collide and fail the entire composition — taking the
// sync loop and its occurrence reconciler down with it, which is the stranding
// the optional fixed loop exists to prevent. With one unconditional
// registration site that collision cannot be expressed at all.
type fixedScheduleGate struct {
	mu      sync.RWMutex
	runtime fixedScheduleRuntime
}

func (gate *fixedScheduleGate) attach(runtime fixedScheduleRuntime) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.runtime = runtime
}

func (gate *fixedScheduleGate) detach() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.runtime = nil
}

// check runs one delegated readiness check under the gate's read lock.
//
// The lock is deliberately held across the delegated call rather than released
// after copying the runtime. Copying first and calling after would let a poll
// observe a healthy runtime, have detach complete, and only then report
// healthy — a result attributable to no point in time, and specifically a
// healthy answer produced after shutdown had already begun. Holding the read
// lock makes detach wait for in-flight checks, so every result is either
// entirely before detach or entirely after it.
//
// Attach was already fail-closed by construction, since an unattached gate
// reports unavailable. This makes detach symmetric.
func (gate *fixedScheduleGate) check(
	ctx context.Context,
	delegate func(fixedScheduleRuntime, context.Context) error,
) error {
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	if gate.runtime == nil {
		return errFixedScheduleUnavailable
	}
	return delegate(gate.runtime, ctx)
}

func (gate *fixedScheduleGate) readiness(ctx context.Context) error {
	return gate.check(ctx, fixedScheduleRuntime.Readiness)
}

func (gate *fixedScheduleGate) coverage(ctx context.Context) error {
	return gate.check(ctx, fixedScheduleRuntime.Coverage)
}

// attached reports whether a runtime is currently bound. It is only for
// assertions that do not delegate; a caller that needs to invoke a check must
// go through check so the result stays linearized against detach.
func (gate *fixedScheduleGate) attached() bool {
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	return gate.runtime != nil
}

type schedulerRuntimeSources struct {
	openDatabase   func(context.Context, config.Config) (schedulerDatabase, error)
	newRepository  func(*pgxpool.Pool) (schedulersync.HandoffStepper, error)
	newCoordinator func() schedulersync.Coordinator
	newLoop        func(
		schedulersync.HandoffStepper,
		schedulersync.Coordinator,
		schedulersync.LoopConfig,
	) (*schedulersync.Loop, error)
	// newFixedLoop builds the fixed maintenance schedule runtime that replaces
	// Celery Beat's non-product entries. It shares this process because two
	// processes must never both believe they own periodic work, and since
	// CHAOS-3114 it takes the COORDINATOR pool: its occurrence ledger is
	// coordinator-exclusive and commits with the producers' domain rows in one
	// transaction.
	newFixedLoop func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error)
	// newOccurrences builds the consumer for the occurrences the sync loop
	// hands off. It is constructed in the same process for the same reason:
	// the marker advances on handoff, so an unconsumed occurrence is stranded
	// work rather than a delayed one. BuildScheduledPlan reads the execution
	// registry directly for plannable identities (CHAOS-4054), so no route
	// configuration is threaded here.
	newOccurrences func(*pgxpool.Pool, *pgxpool.Pool) (schedulersync.OccurrenceStepper, error)
}

var productionSchedulerRuntimeSources = schedulerRuntimeSources{
	openDatabase: openSchedulerDatabase,
	// This branches on the package-level schedulerOwnership variable (main.go)
	// rather than calling schedulersync.NewMutationRepository unconditionally,
	// so that variable is the actual, single source of truth for which
	// ownership policy this binary composes -- not a value that is validated
	// but otherwise ignored. OwnershipPolicy's fields stay unexported in the
	// sync package, so the only two values schedulerOwnership can ever hold
	// are schedulersync.DefaultOwnershipPolicy() and
	// schedulersync.TransferScheduleMarkerOwnershipToGo(); this comparison
	// cannot be fooled into selecting the mutation repository by anything
	// short of a source change to schedulerOwnership itself.
	newRepository: func(pool *pgxpool.Pool) (schedulersync.HandoffStepper, error) {
		if schedulerOwnership == schedulersync.TransferScheduleMarkerOwnershipToGo() {
			return schedulersync.NewMutationRepository(pool)
		}
		return schedulersync.NewRepository(pool)
	},
	newCoordinator: schedulersync.NewOccurrenceCoordinator,
	newLoop:        schedulersync.NewLoop,
	newFixedLoop:   buildFixedScheduleLoop,
	newOccurrences: func(coordinatorPool, domainPool *pgxpool.Pool) (schedulersync.OccurrenceStepper, error) {
		materializer, err := schedulersync.NewNativeMaterializer(domainPool)
		if err != nil {
			return nil, err
		}
		// CHAOS-4060/CHAOS-4114/CHAOS-4124: load the executed-proof snapshot
		// at process startup. A failed load is not fatal to the PROCESS --
		// RefreshExecutedProof installs an empty, non-nil, enforced snapshot on
		// the first failure (fail CLOSED, not open) and maybeRefreshExecutedProof
		// keeps retrying from inside Materialize.
		//
		// But that first-failure state is NOT bounded the way a later failure
		// is. A later failure carries forward Proven facts and HasExecutedProof
		// is checked before Degraded, so proven routes keep planning. A FIRST
		// failure has nothing to carry forward: the empty Degraded snapshot
		// blocks every non-waived pair, which on 2026-08-22 collapsed planning
		// from 17 datasets to the single waived route for eight hours
		// (CHAOS-4124). One 30s attempt against a database busy with recovery
		// load was the entire difference between a healthy deploy and a total
		// outage, so one attempt is not enough. Retry on a short backoff inside
		// a bounded overall budget, then -- if it still has not loaded -- leave
		// the loud ERROR and the fail-closed snapshot exactly as they were and
		// let the readiness check registered by the caller make the deploy
		// visibly unhealthy instead of silently planning nothing.
		refreshErr := loadExecutedProofAtStartup(materializer, time.Sleep)
		if refreshErr != nil {
			slog.Default().Error(
				"executed-proof evidence load failed at scheduler startup after "+
					"every bounded retry; every non-waived route is being treated as "+
					"UNPROVEN (fail closed) and the executed_proof_evidence readiness "+
					"check will hold this process unhealthy until a refresh succeeds "+
					"(CHAOS-4060, CHAOS-4124)",
				"error", refreshErr,
				"attempts", executedProofStartupAttempts,
			)
		}
		return schedulersync.NewOccurrenceReconciler(coordinatorPool, materializer)
	},
}

// executedProofStartupAttempts and the two budgets below bound how hard the
// scheduler tries to load executed-proof evidence before giving up and going
// visibly unready. They are constants rather than environment settings on
// purpose: the numbers only matter during the seconds after a restart, and a
// misconfigured one is exactly the kind of thing nobody notices until the
// next incident.
//
// Per-attempt budget is deliberately SHORTER than the single 30s attempt it
// replaces. Post-CHAOS-4114 this query reads at most one row per route pair
// (~100 rows), so a healthy database answers in milliseconds; a read that has
// not answered in 8s is contending, and the useful response to contention is
// to back off and ask again, not to keep one doomed statement open. The
// overall budget caps total startup delay so a permanently unreachable
// database cannot hang process construction.
const (
	executedProofStartupAttempts       = 6
	executedProofStartupAttemptBudget  = 8 * time.Second
	executedProofStartupTotalBudget    = 45 * time.Second
	executedProofStartupInitialBackoff = 250 * time.Millisecond
	executedProofStartupMaxBackoff     = 4 * time.Second
)

// executedProofStartupLoader is the narrow shape loadExecutedProofAtStartup
// needs, so the retry policy is testable without a database.
type executedProofStartupLoader interface {
	RefreshExecutedProof(context.Context) error
	HasLoadedExecutedProof() bool
}

// loadExecutedProofAtStartup runs the bounded aggressive retry described at
// its only production call site. It returns nil as soon as any attempt
// succeeds and the last error otherwise.
//
// sleep is injected so the test can prove the backoff schedule and the
// bounded attempt count without spending wall-clock time -- and, more
// importantly, so a test can prove the loop STOPS. An unbounded retry here
// would trade CHAOS-4124's silent outage for a process that never finishes
// constructing, which is not an improvement.
func loadExecutedProofAtStartup(
	loader executedProofStartupLoader,
	sleep func(time.Duration),
) error {
	if loader == nil || sleep == nil {
		return errors.New("executed-proof startup loader is required")
	}
	deadline := time.Now().Add(executedProofStartupTotalBudget)
	backoff := executedProofStartupInitialBackoff
	var lastErr error
	for attempt := 1; attempt <= executedProofStartupAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(
			context.Background(), executedProofStartupAttemptBudget,
		)
		err := loader.RefreshExecutedProof(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == executedProofStartupAttempts {
			break
		}
		// Check the overall budget BEFORE sleeping, not after: sleeping past
		// a budget that is already spent adds startup latency that buys
		// nothing, and the whole point of the budget is that a database
		// which is not coming back must not hold the process hostage.
		if !time.Now().Add(backoff).Before(deadline) {
			break
		}
		slog.Default().Warn(
			"executed-proof evidence load failed at scheduler startup; retrying "+
				"before falling back to the fail-closed empty snapshot (CHAOS-4124)",
			"error", err,
			"attempt", attempt,
			"attempts", executedProofStartupAttempts,
			"backoff", backoff,
		)
		sleep(backoff)
		if backoff *= 2; backoff > executedProofStartupMaxBackoff {
			backoff = executedProofStartupMaxBackoff
		}
	}
	return lastErr
}

func buildProductionSchedulerLoop(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	loggers ...*slog.Logger,
) (lifecycle.Component, error) {
	return buildSchedulerLoopWithSources(
		ctx,
		cfg,
		registry,
		productionSchedulerRuntimeSources,
		loggers...,
	)
}

func buildSchedulerLoopWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources schedulerRuntimeSources,
	loggers ...*slog.Logger,
) (lifecycle.Component, error) {
	// Variadic for the same reason the worker's configure path is: existing
	// callers and test doubles stay source-compatible, and a caller that has no
	// logger to give is not forced to invent one.
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	if ctx == nil || registry == nil || sources.openDatabase == nil ||
		sources.newRepository == nil || sources.newCoordinator == nil ||
		sources.newLoop == nil || sources.newFixedLoop == nil ||
		sources.newOccurrences == nil {
		return nil, dependencyUnavailable("scheduler_build_sources_unavailable")
	}
	database, err := sources.openDatabase(ctx, cfg)
	if err != nil && postgres.ConfigurationRejected(err) {
		// A DECLARED configuration rejection -- most commonly no DSN supplied
		// at all -- keeps the process live and unready, exactly as the worker
		// does, so an operator scrapes named readiness failures instead of
		// reading a crash loop (CHAOS-3873). The scheduler previously treated
		// every open failure as fatal, so an unconfigured scheduler container
		// exited before it could publish its operator port at all.
		//
		// The same names the goOwnsMarkers gate closes are closed here, so
		// the externally visible readiness surface does not change shape
		// depending on WHY the loop is not running. execution_liveness
		// (CHAOS-4029) joins that set: an unconfigured scheduler has no
		// domain pool to self-probe, so it must report unavailable rather
		// than silently omitting the check name a configured scheduler
		// would otherwise carry.
		if registerErr := processreadiness.RegisterUnavailable(
			registry,
			"domain_postgres",
			"queue_postgres",
			"coordinator_postgres",
			"river_schema",
			"scheduler_loop",
			"execution_liveness",
		); registerErr != nil {
			return nil, registerErr
		}
		return nil, errSchedulerDatabaseUnconfigured
	}
	if err != nil || database == nil {
		// Operational failure -- unreachable host, refused credentials, an
		// unparseable DSN. Crash-loop rather than idle as an alive-but-unready
		// zombie.
		return nil, dependencyUnavailable("scheduler_domain_database_open_failed")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			database.Close()
		}
	}()
	if err := registry.RegisterRequired("domain_postgres", database.DomainReady); err != nil {
		return nil, err
	}
	if err := registry.RegisterRequired("queue_postgres", database.QueueReady); err != nil {
		return nil, err
	}
	if err := registry.RegisterRequired("coordinator_postgres", database.CoordinatorReady); err != nil {
		return nil, err
	}
	if err := registry.RegisterRequired(
		"river_schema",
		func(ctx context.Context) error {
			return database.RiverSchemaReady(ctx, cfg.RiverDatabaseSchema)
		},
	); err != nil {
		return nil, err
	}
	// CHAOS-3114: the sync handoff repository and occurrence reconciler run
	// on the coordinator pool, not the domain pool. sync_configurations,
	// scheduled_jobs, and scheduled_sync_occurrences are coordinator-exclusive
	// (internal/storage/postgres/domain_authorization.go), and
	// schedulerHandoffCandidatesSQL's `FOR UPDATE OF config, job SKIP LOCKED`
	// requires UPDATE on the locked rows purely to take the lock, even though
	// it writes nothing else -- handing this call site the domain pool fails
	// every handoff with SQLSTATE 42501 (insufficient_privilege). There is
	// deliberately no fallback to the domain pool: CoordinatorPool fails
	// closed with postgres.ErrUnavailable instead.
	coordinatorPool, err := database.CoordinatorPool()
	if err != nil || coordinatorPool == nil {
		return nil, dependencyUnavailable("scheduler_coordinator_pool_unavailable")
	}
	domainPool := database.DomainPool()
	if domainPool == nil {
		return nil, dependencyUnavailable("scheduler_domain_pool_unavailable")
	}
	// execution_liveness (CHAOS-4029): the scheduler's own periodic
	// self-probe against the domain pool, on an independent clock -- the
	// same signal cmd/dev-health-worker and cmd/dev-health-reconciler
	// register, so a scheduler whose handoff/reconcile loop is wedged (but
	// whose dependency checks above still pass) goes visibly unhealthy
	// instead of continuing to report ready while planning nothing. This is
	// deliberately SEPARATE from executed_proof_evidence below:
	// executed_proof_evidence proves the CHAOS-4124 evidence gate loaded;
	// this proves the process's transaction path is still alive at all,
	// which is the precondition executed_proof_evidence's own refresh
	// depends on.
	livenessMonitor := selfprobe.New("scheduler_execution_liveness", selfprobe.NewPool(domainPool), logger)
	if livenessMonitor != nil {
		livenessMonitor.Probe(ctx)
		if err := registry.RegisterRequired("execution_liveness", livenessMonitor.Ready); err != nil {
			return nil, err
		}
		if err := registry.RegisterMetrics("scheduler_execution_liveness", livenessMonitor); err != nil {
			return nil, err
		}
	}
	repository, err := sources.newRepository(coordinatorPool)
	if err != nil || repository == nil {
		return nil, dependencyUnavailable("scheduler_handoff_repository_construction_failed")
	}
	coordinator := sources.newCoordinator()
	if coordinator == nil {
		return nil, dependencyUnavailable("scheduler_occurrence_coordinator_unavailable")
	}
	occurrences, err := sources.newOccurrences(coordinatorPool, domainPool)
	if err != nil || occurrences == nil {
		return nil, dependencyUnavailable("scheduler_occurrence_reconciler_construction_failed")
	}
	// CHAOS-4060: expose the executed-proof gate's own health (refresh
	// failures, degraded state) as a Prometheus source, separate from the
	// scheduler's own readiness -- a degraded gate still completes
	// zero-unit occurrences successfully, so readiness alone cannot surface
	// it. *schedulersync.OccurrenceReconciler (the concrete type behind
	// OccurrenceStepper in production) satisfies this only when its
	// materializer does too (NativeMaterializer does); a test double simply
	// does not match, so this is a no-op outside production composition.
	if source, ok := occurrences.(interface{ WritePrometheus(io.Writer) error }); ok {
		if err := registry.RegisterMetrics("scheduler_executed_proof_gate", source); err != nil {
			return nil, err
		}
	}
	// CHAOS-4124: make a never-loaded evidence snapshot LOUD at the place
	// operators and deploy tooling already look. The outage's defining
	// property was not that the gate failed closed -- that is correct -- but
	// that it did so silently: the process reported ready, completed every
	// occurrence successfully, and planned nothing for eight hours.
	//
	// This is deliberately a readiness check and not a construction failure.
	// Refusing to construct would turn an evidence outage into a scheduler
	// outage, and refusing to RUN would reproduce CHAOS-4124 by another
	// route. The scheduler's loop does not gate on Registry.CheckRequired
	// (only the worker does, cmd/dev-health-worker/dependencies.go), so a
	// failing check here surfaces on /ready and in
	// dev_health_runtime_check_failed while planning continues on whatever
	// evidence exists -- the deploy goes visibly unhealthy instead of
	// silently degrading. maybeRefreshExecutedProof keeps retrying, so the
	// check clears itself the moment a refresh succeeds; nothing has to be
	// restarted to recover.
	if reporter, ok := occurrences.(interface{ HasLoadedExecutedProof() bool }); ok {
		if err := registry.RegisterRequired(
			"executed_proof_evidence",
			func(context.Context) error {
				if reporter.HasLoadedExecutedProof() {
					return nil
				}
				return errors.New(
					"executed-proof evidence has never loaded in this process; the " +
						"gate is running on an empty Degraded snapshot and every " +
						"non-waived provider/dataset pair is blocked from planning " +
						"(CHAOS-4124)",
				)
			},
		); err != nil {
			return nil, err
		}
	}
	loop, err := sources.newLoop(
		repository,
		coordinator,
		schedulersync.DefaultLoopConfig(registry).WithOccurrences(occurrences).WithLogger(logger),
	)
	if err != nil || loop == nil {
		return nil, dependencyUnavailable("scheduler_sync_loop_construction_failed")
	}
	// The fixed maintenance scheduler is deliberately optional. Product schedule
	// handoff and pending-occurrence reconciliation are a different workload
	// with a different failure mode: an occurrence whose marker has already
	// advanced is stranded until something consumes it, so tying its fate to
	// whether Beat's maintenance replacement could be constructed would turn a
	// maintenance outage into permanently unprocessed product work.
	//
	// Its readiness names are claimed here, before construction is attempted, so
	// a construction failure needs no compensating registration and therefore
	// cannot collide with one the failed constructor already made.
	gate := &fixedScheduleGate{}
	if err := registry.RegisterRequired("fixed_scheduler_loop", gate.readiness); err != nil {
		return nil, err
	}
	if err := registry.RegisterRequired("fixed_schedule_coverage", gate.coverage); err != nil {
		return nil, err
	}
	// CHAOS-3114 (second half): the fixed maintenance engine runs on the
	// coordinator pool too, on the same fail-closed coordinatorPool obtained
	// above -- there is no domain-pool fallback here either. runOccurrence
	// commits fixed_schedule_occurrences (coordinator-exclusive) together with
	// remaining_metric_runs, remaining_metric_partitions,
	// work_graph_execution_requests, worker_job_outbox and the completion
	// fence in ONE transaction, and the atomicity is what makes "occurrence
	// recorded => work enqueued" exactly-once. Splitting it outbox-style would
	// let a fixed schedule double-run or silently skip, so the coordinator
	// posture was widened to cover the whole statement set instead
	// (internal/storage/postgres/domain_authorization.go coordinatorPosture).
	// The domain role deliberately did NOT gain fixed_schedule_occurrences:
	// widening the login that provider-sync workers use is what the partition
	// exists to prevent.
	fixedLoop, err := sources.newFixedLoop(coordinatorPool, registry, logger)
	if err != nil || fixedLoop == nil {
		// The gate stays unattached, so both names report unavailable while the
		// product loop below runs normally.
		fixedLoop = nil
	}
	closeOnError = false
	return schedulerRuntime{
		database:        database,
		loop:            loop,
		fixedLoop:       fixedLoop,
		fixedGate:       gate,
		livenessMonitor: livenessMonitor,
	}, nil
}

type schedulerRuntime struct {
	database        schedulerDatabase
	loop            *schedulersync.Loop
	fixedLoop       fixedScheduleRuntime
	fixedGate       *fixedScheduleGate
	livenessMonitor *selfprobe.Monitor
}

func (schedulerRuntime) Name() string { return "sync-scheduler-runtime" }

// Start brings up the product schedule loop, which owns marker handoff and
// pending-occurrence reconciliation. That loop is required: without it an
// activated scheduler has no owner for either.
//
// The fixed maintenance loop starts after it and is allowed to fail. Its
// failure closes its own readiness names and leaves the product loop running,
// because a pending occurrence whose marker already advanced must still be
// consumed while maintenance schedules are broken or intentionally disabled.
func (component schedulerRuntime) Start(ctx context.Context) error {
	if component.database == nil || component.loop == nil {
		return errSchedulerActivationUnavailable
	}
	if err := component.loop.Start(ctx); err != nil {
		return err
	}
	// Starting the self-probe's background ticker is best-effort and never
	// fails Start: Monitor.Start's return is always nil (see its doc
	// comment), matching queueHealthMonitor's "never block startup on a
	// dependency that already has its own required check" rule.
	if component.livenessMonitor != nil {
		_ = component.livenessMonitor.Start(ctx)
	}
	if component.fixedLoop == nil || component.fixedGate == nil {
		return nil
	}
	if err := component.fixedLoop.Start(ctx); err != nil {
		// Leaving the gate unattached reports the degradation through the fixed
		// profile's own readiness names without abandoning product scheduling.
		component.fixedGate.detach()
		return nil
	}
	component.fixedGate.attach(component.fixedLoop)
	return nil
}

// Shutdown stops whichever loops are running and always closes the database,
// even when one fails to stop cleanly.
func (component schedulerRuntime) Shutdown(ctx context.Context) error {
	if component.database != nil {
		defer component.database.Close()
	}
	if component.loop == nil {
		return errSchedulerActivationUnavailable
	}
	var fixedErr, livenessErr error
	if component.fixedGate != nil {
		component.fixedGate.detach()
	}
	if component.fixedLoop != nil {
		fixedErr = component.fixedLoop.Shutdown(ctx)
	}
	if component.livenessMonitor != nil {
		livenessErr = component.livenessMonitor.Shutdown(ctx)
	}
	return errors.Join(fixedErr, livenessErr, component.loop.Shutdown(ctx))
}
