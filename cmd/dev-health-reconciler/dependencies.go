package main

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
)

const (
	defaultReconcilerContractRoot   = "contracts/jobs/v1"
	defaultSyncDispatchContractRoot = "contracts/sync-dispatch/v1"
	recorderCleanupTimeout          = time.Second
)

var errReconcilerDependencyUnavailable = errors.New("reconciler readiness dependency is unavailable")

// dependencyFailure attaches a bounded reason code to the generic dependency
// sentinel, mirroring cmd/dev-health-worker/dependencies.go's dependencyFailure
// exactly. Before this, every distinct construction failure in this binary --
// a database that would not open, a missing job registry, a broken sync
// dispatch pipeline -- collapsed into the same bare
// errReconcilerDependencyUnavailable, so an operator could not tell which
// knob was wrong (CHAOS-3873/CHAOS-3907). The reason is always a
// compile-time constant, never interpolated input, so logging it cannot leak
// a DSN or a secret.
type dependencyFailure struct {
	reason string
}

func (failure dependencyFailure) Error() string {
	return errReconcilerDependencyUnavailable.Error() + ": " + failure.reason
}

func (dependencyFailure) Unwrap() error { return errReconcilerDependencyUnavailable }

// DependencyReason satisfies the shell's reason-code interface.
func (failure dependencyFailure) DependencyReason() string { return failure.reason }

func dependencyUnavailable(reason string) error { return dependencyFailure{reason: reason} }

// reconcilerDatabase keeps the command's domain, queue-control, and
// coordinator trust boundaries testable without weakening the production
// RuntimePools contract.
type reconcilerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	CoordinatorReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	DomainPool() *pgxpool.Pool
	QueuePool() *pgxpool.Pool
	CoordinatorPool() *pgxpool.Pool
	Close()
}

type postgresReconcilerDatabase struct {
	pools           *postgres.RuntimePools
	domainRole      string
	queueRole       string
	coordinatorRole string
	riverSchema     string
}

func openReconcilerDatabase(ctx context.Context, cfg config.Config) (reconcilerDatabase, error) {
	// The reconciler is a coordinator binary: deploy/go-workers/deployment.json
	// gives its "control" runtime coordinator_max_connections >= 1, and its
	// always-constructed outbox relay reads worker_job_routes, which is
	// coordinator-exclusive. WithCoordinator makes the coordinator DSN a
	// startup requirement rather than an optional extra.
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg).WithCoordinator()
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresReconcilerDatabase{
		pools: pools, domainRole: runtimeConfig.DomainRole, queueRole: runtimeConfig.QueueRole,
		coordinatorRole: runtimeConfig.CoordinatorRole, riverSchema: runtimeConfig.RiverSchema,
	}, nil
}

func (database *postgresReconcilerDatabase) DomainReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Domain == nil {
		return errReconcilerDependencyUnavailable
	}
	return postgres.CheckDomainAuthorization(ctx, database.pools.Domain, database.domainRole, database.riverSchema)
}

func (database *postgresReconcilerDatabase) QueueReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errReconcilerDependencyUnavailable
	}
	return postgres.CheckQueueAuthorization(ctx, database.pools.QueueControl, database.queueRole, database.riverSchema)
}

// CoordinatorReady proves the coordinator login holds exactly
// coordinatorPosture. It is a separate readiness gate from DomainReady on
// purpose: cross-role attribution is distributed, so only the coordinator's own
// check can catch a privilege wrongly granted to the coordinator role.
func (database *postgresReconcilerDatabase) CoordinatorReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Coordinator == nil {
		return errReconcilerDependencyUnavailable
	}
	return postgres.CheckCoordinatorAuthorization(
		ctx, database.pools.Coordinator, database.coordinatorRole, database.riverSchema,
	)
}

func (database *postgresReconcilerDatabase) RiverSchemaReady(ctx context.Context, schema string) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errReconcilerDependencyUnavailable
	}
	_, err := riverstore.CheckSchema(ctx, database.pools.QueueControl, schema, nil)
	return err
}

func (database *postgresReconcilerDatabase) QueuePool() *pgxpool.Pool {
	if database == nil || database.pools == nil {
		return nil
	}
	return database.pools.QueueControl
}

func (database *postgresReconcilerDatabase) DomainPool() *pgxpool.Pool {
	if database == nil || database.pools == nil {
		return nil
	}
	return database.pools.Domain
}

// CoordinatorPool never falls back to the domain pool. A nil return propagates
// as a dependency failure, which is the intended behaviour: silently handing a
// coordinator call site the domain pool is the defect this split removes.
func (database *postgresReconcilerDatabase) CoordinatorPool() *pgxpool.Pool {
	if database == nil || database.pools == nil {
		return nil
	}
	return database.pools.Coordinator
}

func (database *postgresReconcilerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

type reconcilerDependencySources struct {
	openDatabase        func(context.Context, config.Config) (reconcilerDatabase, error)
	loadRuntimeRegistry func(string) (*jobruntime.Registry, error)
	buildRelay          func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error)
	newLoop             func(joboutbox.RelayStepper, joboutbox.ReconcilerLoopConfig) (*joboutbox.ReconcilerLoop, error)
	contractRoot        string

	loadSyncDispatchRegistry func(string) (*syncdispatchcontract.Registry, error)
	buildSyncRouteFence      func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncroute.Checker, error)
	buildSyncShadow          func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error)
	buildSyncMutation        func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *syncdispatchcontract.Registry, config.Config) (syncreconciler.Stepper, error)
	newSyncRecorder          func(*slog.Logger) (reconcilerObservationRecorder, error)
	newSyncLoop              func(syncreconciler.Stepper, syncreconciler.LoopConfig) (*syncreconciler.Loop, error)
	syncDispatchContractRoot string
}

// reconcilerActivation is a source-reviewed composition seam. It is
// deliberately not configurable through environment or deployment groups:
// changing from observation to mutation must retain concrete River delivery
// capabilities in the same reviewed source change.
type reconcilerActivation struct {
	syncMutation bool
}

// Every checked-in sync-dispatch route is River-owned. The reconciler must run
// the mutation pipeline so durable outbox wakeups are claimed and published;
// leaving this in shadow mode strands planned runs when Python workers are not
// present even though the route table says River.
var checkedInReconcilerActivation = reconcilerActivation{syncMutation: true}

var productionReconcilerDependencySources = reconcilerDependencySources{
	openDatabase:             openReconcilerDatabase,
	loadRuntimeRegistry:      jobruntime.Load,
	buildRelay:               buildReconcilerRelay,
	newLoop:                  joboutbox.NewReconcilerLoop,
	contractRoot:             defaultReconcilerContractRoot,
	loadSyncDispatchRegistry: syncdispatchcontract.Load,
	buildSyncRouteFence: func(pool *pgxpool.Pool, registry *syncdispatchcontract.Registry) (syncroute.Checker, error) {
		return syncroute.New(pool, registry)
	},
	buildSyncShadow: func(pool *pgxpool.Pool, registry *syncdispatchcontract.Registry) (syncreconciler.Stepper, error) {
		return syncreconciler.NewShadow(pool, registry)
	},
	buildSyncMutation: buildSyncMutationPipeline,
	newSyncRecorder: func(logger *slog.Logger) (reconcilerObservationRecorder, error) {
		return syncreconciler.NewSlogObservationRecorder(logger)
	},
	newSyncLoop:              syncreconciler.NewLoop,
	syncDispatchContractRoot: defaultSyncDispatchContractRoot,
}

// The active mutation pipeline is wired so activation does not ship a 42501:
// the Materializer
// reads sync_run_reference_discoveries and sync_run_post_dispatches, both
// coordinator-exclusive, so it takes the coordinator pool. LeaseRepair, the
// Kernel's observe side, and the Observer stay on the domain pool -- every
// table they touch is domain-granted, and widening them would defeat the split.
func buildSyncMutationPipeline(
	coordinatorPool *pgxpool.Pool,
	domainPool *pgxpool.Pool,
	queuePool *pgxpool.Pool,
	riverSchema string,
	registry *syncdispatchcontract.Registry,
	cfg config.Config,
) (syncreconciler.Stepper, error) {
	repair, err := syncreconciler.NewLeaseRepair(domainPool)
	if err != nil {
		return nil, err
	}
	sweep, err := buildUnreclaimableSweep(domainPool, cfg)
	if err != nil {
		return nil, err
	}
	terminalRepair, err := syncreconciler.NewTerminalDeliveryRepair(queuePool, riverSchema)
	if err != nil {
		return nil, err
	}
	materializer, err := syncreconciler.NewMaterializer(coordinatorPool)
	if err != nil {
		return nil, err
	}
	riverClient, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{
		Schema: riverSchema,
		// This client only inserts sync-dispatch coordinator jobs; otelriver
		// still gives the insert side of that boundary a river.insert_many
		// span, matching the other River client constructions (CHAOS-3993).
		Middleware: []rivertype.Middleware{otelriver.NewMiddleware(nil)},
	})
	if err != nil {
		return nil, err
	}
	publisher, err := syncdispatchruntime.NewPublisher(riverClient, syncdispatchruntime.PublisherOptions{
		Queue: syncdispatchcontract.RiverQueue, MaxAttempts: 5,
	})
	if err != nil {
		return nil, err
	}
	kernel, err := syncreconciler.NewKernel(
		domainPool,
		queuePool,
		registry,
		syncreconciler.KernelModeMutation,
	)
	if err != nil {
		return nil, err
	}
	observer, err := syncreconciler.NewObserver(domainPool, registry)
	if err != nil {
		return nil, err
	}
	publish := func(ctx context.Context, tx pgx.Tx, claim syncreconciler.TransportClaim) (string, error) {
		reference, referenceErr := syncDispatchReference(ctx, domainPool, claim.ID, claim.Kind)
		if referenceErr != nil {
			return "", referenceErr
		}
		return publisher.Publish(ctx, tx, syncDispatchClaimForTransport(claim), reference)
	}
	// The worker registers all four coordinator kinds, including the native
	// post_sync fanout, before advertising River route readiness.
	return syncreconciler.NewMutationPipeline(
		repair,
		terminalRepair,
		materializer,
		kernel,
		observer,
		publish,
		nil,
		sweep,
		syncreconciler.DefaultMutationPipelineConfig(),
	)
}

func syncDispatchClaimForTransport(claim syncreconciler.TransportClaim) syncdispatchruntime.Claim {
	return syncdispatchruntime.Claim{
		OutboxID: claim.ID, Kind: claim.Kind, RouteGeneration: claim.RouteGeneration,
		DeliveryAttempt: claim.Attempts,
	}
}

func syncDispatchReference(
	ctx context.Context,
	pool *pgxpool.Pool,
	outboxID string,
	kind string,
) (syncdispatchruntime.DomainReference, error) {
	if pool == nil {
		return syncdispatchruntime.DomainReference{}, syncreconciler.ErrUnavailable
	}
	if _, err := uuid.Parse(outboxID); err != nil {
		return syncdispatchruntime.DomainReference{}, syncreconciler.ErrUnavailable
	}
	var reference syncdispatchruntime.DomainReference
	var traceParent *string
	// The join reads sync_runs.trace_parent (CHAOS-3996): the W3C traceparent
	// the Python planner captured once when this run was planned, so every
	// dispatch across the run's lifecycle -- however many
	// dispatch/finalize/post_sync/reference_discovery cycles it takes --
	// parents its span from the same trace instead of each claim resolving
	// its own. NULL for a run planned before that column existed or with
	// tracing disabled; traceParent stays nil and the caller gets a root span.
	err := pool.QueryRow(ctx, `
SELECT sdo.org_id::text, sdo.sync_run_id::text, sr.trace_parent
FROM public.sync_dispatch_outbox sdo
JOIN public.sync_runs sr ON sr.id = sdo.sync_run_id
WHERE sdo.id = $1::uuid AND sdo.kind = $2`, outboxID, kind).Scan(&reference.OrganizationID, &reference.SyncRunID, &traceParent)
	if err != nil {
		return syncdispatchruntime.DomainReference{}, syncreconciler.ErrUnavailable
	}
	if traceParent != nil {
		reference.TraceParent = *traceParent
	}
	if _, orgErr := uuid.Parse(reference.OrganizationID); orgErr != nil {
		return syncdispatchruntime.DomainReference{}, syncreconciler.ErrUnavailable
	}
	if _, runErr := uuid.Parse(reference.SyncRunID); runErr != nil {
		return syncdispatchruntime.DomainReference{}, syncreconciler.ErrUnavailable
	}
	return reference, nil
}

// reconcilerObservationRecorder is the command-owned recorder seam. The
// sync observer loop only offers observations; this command owns lifecycle
// shutdown so its worker cannot outlive the database pools it observes.
type reconcilerObservationRecorder interface {
	syncreconciler.ObservationRecorder
	Shutdown(context.Context) error
}

// The relay's route resolver runs on the COORDINATOR pool. Every relay step
// calls DeferredKinds -> Resolve -> Inspect, which SELECTs
// public.worker_job_routes; that table is coordinator-exclusive under the
// Option B split, so on the domain pool this is an unconditional 42501 on the
// reconciler's hot path, not a rare operator error. The repository, River
// inserter, and River quiescer stay on the queue pool, which owns the outbox
// drain and the River schema.
func buildReconcilerRelay(
	coordinatorPool *pgxpool.Pool,
	domainPool *pgxpool.Pool,
	queuePool *pgxpool.Pool,
	riverSchema string,
	registry *jobruntime.Registry,
) (joboutbox.RelayStepper, error) {
	repository, err := joboutbox.NewRepository(queuePool)
	if err != nil {
		return nil, err
	}
	inserter, err := joboutbox.NewRiverInserter(queuePool, riverSchema, registry)
	if err != nil {
		return nil, err
	}
	quiescer, err := jobroute.NewPostgresRiverQuiescer(queuePool, riverSchema)
	if err != nil {
		return nil, err
	}
	routes, err := jobroute.NewController(coordinatorPool, registry, quiescer)
	if err != nil {
		return nil, err
	}
	repair, err := joboutbox.NewTerminalDeliveryRepair(queuePool, riverSchema)
	if err != nil {
		return nil, err
	}
	// The strand sweep spans TWO pools by design. Selection and rearm run on
	// the queue pool, which owns the outbox and the River schema and which
	// needs the daily-metrics and work-graph SELECT grants added alongside this
	// (internal/storage/river/migrate.go is the authority;
	// internal/storage/postgres/queue_authorization.go asserts the matching
	// posture). The execution-state read runs on the DOMAIN pool, because the
	// queue-control role must never see worker_job_runs -- the domain role
	// already holds SELECT on it, so the read adds no privilege anywhere. The
	// domain pool is reused from the reconciler's existing dependencies rather
	// than opened again. Without the queue grants every pass returns
	// joboutbox.ErrNotAuthorized rather than a silent zero.
	strandRepair, err := joboutbox.NewStrandRepair(queuePool, domainPool, riverSchema)
	if err != nil {
		return nil, err
	}
	return joboutbox.NewRelayWithRoutesRecoveryAndStrandRepair(
		repository, inserter, routes, repair, strandRepair, joboutbox.DefaultRelayConfig(),
	)
}

type reconcilerDependencies struct {
	database    reconcilerDatabase
	databaseErr error

	// logger and coordinatorRole exist only to make a coordinator_postgres
	// readiness failure explain itself (see coordinatorReady /
	// logCoordinatorPostureGaps). Neither is a DSN, host, or credential --
	// coordinatorRole is a checked-in configuration identifier, and logger is
	// the same structured logger the rest of the process already uses.
	logger          *slog.Logger
	coordinatorRole string

	runtimeRegistry *jobruntime.Registry
	registryErr     error
	relayErr        error
	loop            *joboutbox.ReconcilerLoop
	loopErr         error

	syncDispatchRegistry *syncdispatchcontract.Registry
	syncRegistryErr      error
	syncRouteFence       syncroute.Checker
	syncRouteFenceErr    error
	syncObserverErr      error
	syncRecorder         reconcilerObservationRecorder
	syncRecorderErr      error
	syncLoop             *syncreconciler.Loop
	syncLoopErr          error
}

func configureReconcilerDependenciesWithSourcesAndLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
	sources reconcilerDependencySources,
) ([]lifecycle.Component, error) {
	return configureReconcilerDependenciesWithActivationSourcesAndLogger(
		ctx,
		cfg,
		registry,
		logger,
		checkedInReconcilerActivation,
		sources,
	)
}

func configureReconcilerDependenciesWithActivationSourcesAndLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
	activation reconcilerActivation,
	sources reconcilerDependencySources,
) ([]lifecycle.Component, error) {
	if registry == nil {
		return nil, dependencyUnavailable("reconciler_registry_unavailable")
	}

	dependencies := buildReconcilerDependencies(ctx, cfg, registry, logger, activation, sources)
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "job_registry", check: dependencies.registryReady},
		{name: "queue_postgres", check: dependencies.queueReady},
		// Named separately from domain_postgres so a coordinator-role privilege
		// problem is attributable in readiness output rather than surfacing as a
		// generic domain failure.
		{name: "coordinator_postgres", check: dependencies.coordinatorReady},
		{name: "river_schema", check: dependencies.riverSchemaReady(cfg.RiverDatabaseSchema)},
		{name: "sync_dispatch_registry", check: dependencies.syncRegistryReady},
	}
	// If prerequisite construction failed, the existing domain/registry checks
	// already close readiness. Once fence construction was attempted, register
	// its own named check so runtime route drift remains independently visible.
	if dependencies.syncRouteFence != nil || dependencies.syncRouteFenceErr != nil {
		checks = append(checks, struct {
			name  string
			check health.CheckFunc
		}{name: "sync_dispatch_route_fence", check: dependencies.syncRouteFenceReady})
	}
	for _, check := range checks {
		if err := registry.RegisterRequired(check.name, check.check); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	if dependencies.loop == nil {
		if err := registry.RegisterRequired("reconciler_loop", dependencies.reconcilerReady); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	if dependencies.syncLoop == nil {
		if err := registry.RegisterRequired("sync_dispatch_observer", dependencies.syncObserverReady); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	if dependencies.database == nil || dependencies.loop == nil || dependencies.syncLoop == nil || dependencies.syncRecorder == nil {
		dependencies.close()
		return nil, nil
	}
	return []lifecycle.Component{
		reconcilerDatabaseLifecycle{database: dependencies.database},
		dependencies.loop,
		reconcilerRecorderLifecycle{recorder: dependencies.syncRecorder},
		dependencies.syncLoop,
	}, nil
}

func buildReconcilerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
	activation reconcilerActivation,
	sources reconcilerDependencySources,
) *reconcilerDependencies {
	dependencies := &reconcilerDependencies{logger: logger, coordinatorRole: cfg.CoordinatorDatabaseRole}
	if sources.openDatabase == nil {
		dependencies.databaseErr = dependencyUnavailable("reconciler_database_source_missing")
	} else {
		dependencies.database, dependencies.databaseErr = sources.openDatabase(ctx, cfg)
		if dependencies.databaseErr != nil {
			dependencies.databaseErr = dependencyUnavailable("reconciler_database_open_failed")
			dependencies.disableDatabase()
		}
	}
	if sources.loadRuntimeRegistry == nil || sources.contractRoot == "" {
		dependencies.registryErr = dependencyUnavailable("reconciler_job_registry_source_missing")
	} else {
		dependencies.runtimeRegistry, dependencies.registryErr = sources.loadRuntimeRegistry(sources.contractRoot)
	}
	if sources.loadSyncDispatchRegistry == nil || sources.syncDispatchContractRoot == "" {
		dependencies.syncRegistryErr = dependencyUnavailable("reconciler_sync_dispatch_registry_source_missing")
	} else {
		dependencies.syncDispatchRegistry, dependencies.syncRegistryErr = sources.loadSyncDispatchRegistry(sources.syncDispatchContractRoot)
	}
	if dependencies.databaseErr != nil || dependencies.database == nil ||
		dependencies.registryErr != nil || dependencies.runtimeRegistry == nil ||
		dependencies.syncRegistryErr != nil || dependencies.syncDispatchRegistry == nil ||
		sources.buildRelay == nil || sources.newLoop == nil ||
		sources.buildSyncRouteFence == nil ||
		(!activation.syncMutation && sources.buildSyncShadow == nil) ||
		(activation.syncMutation && sources.buildSyncMutation == nil) ||
		sources.newSyncRecorder == nil ||
		sources.newSyncLoop == nil {
		dependencies.relayErr = dependencyUnavailable("reconciler_build_sources_missing")
		dependencies.disableDatabase()
		return dependencies
	}

	relay, err := sources.buildRelay(
		dependencies.database.CoordinatorPool(),
		dependencies.database.DomainPool(),
		dependencies.database.QueuePool(),
		cfg.RiverDatabaseSchema,
		dependencies.runtimeRegistry,
	)
	if err != nil || relay == nil {
		dependencies.relayErr = dependencyUnavailable("reconciler_relay_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	loopConfig := joboutbox.DefaultReconcilerLoopConfig(registry)
	loopConfig.Logger = logger
	loop, err := sources.newLoop(relay, loopConfig)
	if err != nil || loop == nil {
		dependencies.loopErr = dependencyUnavailable("reconciler_relay_loop_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	dependencies.loop = loop
	routeFence, err := sources.buildSyncRouteFence(dependencies.database.DomainPool(), dependencies.syncDispatchRegistry)
	if err != nil || routeFence == nil {
		dependencies.syncRouteFenceErr = dependencyUnavailable("reconciler_sync_route_fence_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	dependencies.syncRouteFence = routeFence
	var syncStepper syncreconciler.Stepper
	if activation.syncMutation {
		syncStepper, err = sources.buildSyncMutation(
			dependencies.database.CoordinatorPool(),
			dependencies.database.DomainPool(),
			dependencies.database.QueuePool(),
			cfg.RiverDatabaseSchema,
			dependencies.syncDispatchRegistry,
			cfg,
		)
	} else {
		syncStepper, err = sources.buildSyncShadow(
			dependencies.database.DomainPool(),
			dependencies.syncDispatchRegistry,
		)
	}
	if err != nil || syncStepper == nil {
		dependencies.syncObserverErr = dependencyUnavailable("reconciler_sync_stepper_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	if logger == nil {
		dependencies.syncRecorderErr = dependencyUnavailable("reconciler_sync_recorder_logger_missing")
		dependencies.disableDatabase()
		return dependencies
	}
	recorder, err := sources.newSyncRecorder(logger)
	dependencies.syncRecorder = recorder
	if err != nil || recorder == nil {
		dependencies.syncRecorderErr = dependencyUnavailable("reconciler_sync_recorder_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	syncLoopConfig := syncreconciler.DefaultLoopConfig(registry)
	syncLoopConfig.Recorder = recorder
	syncLoopConfig.Logger = logger
	syncLoop, err := sources.newSyncLoop(syncStepper, syncLoopConfig)
	if err != nil || syncLoop == nil {
		dependencies.syncLoopErr = dependencyUnavailable("reconciler_sync_loop_construction_failed")
		dependencies.disableDatabase()
		return dependencies
	}
	dependencies.syncLoop = syncLoop
	return dependencies
}

func (dependencies *reconcilerDependencies) domainReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errReconcilerDependencyUnavailable
	}
	if err := dependencies.database.DomainReady(ctx); err != nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) queueReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errReconcilerDependencyUnavailable
	}
	if err := dependencies.database.QueueReady(ctx); err != nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) coordinatorReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errReconcilerDependencyUnavailable
	}
	if err := dependencies.database.CoordinatorReady(ctx); err != nil {
		dependencies.logCoordinatorPostureGaps(ctx)
		return errReconcilerDependencyUnavailable
	}
	return nil
}

// logCoordinatorPostureGaps re-derives, in a separate best-effort diagnostic
// query, which of the coordinator role's declared requirements are currently
// unsatisfied, and logs them at ERROR. Before this existed, a
// coordinator_postgres readiness failure surfaced only as a check name at
// the /readyz HTTP surface with no reason in the process logs either --
// diagnosing CHAOS-3142's actual cause (one required table missing because
// alembic was two migrations behind head) took manually re-deriving and
// re-running postgres.CoordinatorPosture()'s table list by hand against the
// live database. This closes that gap.
//
// It never logs a DSN, host, or credential: postgres.PostureGap is built
// entirely from checked-in table/column identifiers (schema identifiers
// already committed to this repository, not connection material) and
// standard SQL privilege keywords -- see
// internal/storage/postgres/posture_diagnostics_integration_test.go's
// TestDiagnoseRolePostureNamesTheGapAndNeverLeaksConnectionMaterial, which
// renders every gap the same way this function does and asserts the
// rendered line never contains a real connection URI or password.
//
// Best-effort and silent on its own failure: losing the diagnostic must
// never turn an otherwise-handled readiness failure into a harder one, and
// this runs on every failed poll, so it must never itself become a new
// failure mode.
func (dependencies *reconcilerDependencies) logCoordinatorPostureGaps(ctx context.Context) {
	if dependencies == nil || dependencies.logger == nil || dependencies.database == nil {
		return
	}
	pool := dependencies.database.CoordinatorPool()
	if pool == nil {
		return
	}
	gaps, err := postgres.DiagnoseRolePosture(ctx, pool, dependencies.coordinatorRole, postgres.CoordinatorPosture())
	if err != nil || len(gaps) == 0 {
		return
	}
	details := make([]string, len(gaps))
	for i, gap := range gaps {
		details[i] = gap.String()
	}
	dependencies.logger.ErrorContext(ctx, "coordinator readiness posture gap",
		"check", "coordinator_postgres",
		"gaps", details,
	)
}

func (dependencies *reconcilerDependencies) registryReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil || dependencies.runtimeRegistry == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) riverSchemaReady(schema string) health.CheckFunc {
	return func(ctx context.Context) error {
		if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
			return errReconcilerDependencyUnavailable
		}
		if err := dependencies.database.RiverSchemaReady(ctx, schema); err != nil {
			return errReconcilerDependencyUnavailable
		}
		return nil
	}
}

func (dependencies *reconcilerDependencies) reconcilerReady(context.Context) error {
	if dependencies == nil || dependencies.relayErr != nil || dependencies.loopErr != nil || dependencies.loop == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) syncRegistryReady(context.Context) error {
	if dependencies == nil || dependencies.syncRegistryErr != nil || dependencies.syncDispatchRegistry == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) syncRouteFenceReady(ctx context.Context) error {
	if dependencies == nil || dependencies.syncRouteFenceErr != nil || dependencies.syncRouteFence == nil {
		return errReconcilerDependencyUnavailable
	}
	if err := dependencies.syncRouteFence.Check(ctx); err != nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) syncObserverReady(context.Context) error {
	if dependencies == nil || dependencies.syncObserverErr != nil || dependencies.syncRecorderErr != nil || dependencies.syncLoopErr != nil || dependencies.syncLoop == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (dependencies *reconcilerDependencies) close() {
	if dependencies == nil {
		return
	}
	if dependencies.syncRecorder != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), recorderCleanupTimeout)
		_ = dependencies.syncRecorder.Shutdown(cleanupCtx)
		cleanupCancel()
		dependencies.syncRecorder = nil
	}
	if dependencies.database != nil {
		dependencies.database.Close()
	}
}

func (dependencies *reconcilerDependencies) disableDatabase() {
	if dependencies == nil {
		return
	}
	dependencies.close()
	dependencies.database = nil
	if dependencies.databaseErr == nil {
		dependencies.databaseErr = dependencyUnavailable("reconciler_database_disabled_after_dependency_failure")
	}
}

type reconcilerDatabaseLifecycle struct {
	database reconcilerDatabase
}

type reconcilerRecorderLifecycle struct {
	recorder reconcilerObservationRecorder
}

func (reconcilerRecorderLifecycle) Name() string { return "sync-dispatch-observation-recorder" }

func (component reconcilerRecorderLifecycle) Start(context.Context) error {
	if component.recorder == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (component reconcilerRecorderLifecycle) Shutdown(ctx context.Context) error {
	if component.recorder == nil {
		return errReconcilerDependencyUnavailable
	}
	return component.recorder.Shutdown(ctx)
}

func (reconcilerDatabaseLifecycle) Name() string { return "postgres-runtime-pools" }

func (component reconcilerDatabaseLifecycle) Start(context.Context) error {
	if component.database == nil {
		return errReconcilerDependencyUnavailable
	}
	return nil
}

func (component reconcilerDatabaseLifecycle) Shutdown(context.Context) error {
	if component.database != nil {
		component.database.Close()
	}
	return nil
}

// buildUnreclaimableSweep wires the CHAOS-4005 safety net.
//
// Mode is the only knob, and it carries the whole decision:
//
//   - off     -- disabled outright;
//   - shadow  -- DEFAULT. Selects and reports what it would terminalize, and
//     writes nothing, so every deployment gets the observability
//     with no write risk and no activation step;
//   - active  -- permitted to terminalize. Setting it IS the operator's
//     declaration that no Celery consumer serves provider units
//     for this deployment.
//
// An earlier cut read EXPECTED_WORKER_GROUPS as that declaration. That was
// wrong twice over: the variable's own contract explicitly excludes the
// reconciler (deploy/kubernetes/go-workers.yaml, above its definition), and a
// second variable asserting what the mode already asserts is the env sprawl
// CHAOS-4020 exists to remove.
//
// Rollback safety does not rest on the mode at all: the sweep reads the
// durable worker_job_routes row and declines unless River owns provider units,
// so a CUT-19 rollback is covered by mechanism rather than by an operator
// remembering to flip something back.
func buildUnreclaimableSweep(
	domainPool *pgxpool.Pool,
	cfg config.Config,
) (syncreconciler.UnreclaimableSweepStepper, error) {
	mode, err := syncreconciler.ParseSweepMode(cfg.UnreclaimableSweepMode)
	if err != nil {
		return nil, err
	}
	if mode == syncreconciler.SweepModeOff {
		return nil, nil
	}
	return syncreconciler.NewUnreclaimableSweep(domainPool, syncreconciler.UnreclaimableSweepConfig{
		Age:      syncreconciler.DefaultUnreclaimableAge,
		Idle:     syncreconciler.DefaultUnreclaimableIdle,
		Mode:     mode,
		Switches: providersync.RouteSwitchesFromConfig(cfg),
	})
}
