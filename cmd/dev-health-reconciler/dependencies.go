package main

import (
	"context"
	"errors"
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
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultReconcilerContractRoot   = "contracts/jobs/v1"
	defaultSyncDispatchContractRoot = "contracts/sync-dispatch/v1"
	recorderCleanupTimeout          = time.Second
)

var errReconcilerDependencyUnavailable = errors.New("reconciler readiness dependency is unavailable")

// reconcilerDatabase keeps the command's domain and queue-control trust
// boundaries testable without weakening the production RuntimePools contract.
type reconcilerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	DomainPool() *pgxpool.Pool
	QueuePool() *pgxpool.Pool
	Close()
}

type postgresReconcilerDatabase struct {
	pools       *postgres.RuntimePools
	domainRole  string
	queueRole   string
	riverSchema string
}

func openReconcilerDatabase(ctx context.Context, cfg config.Config) (reconcilerDatabase, error) {
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg)
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresReconcilerDatabase{
		pools: pools, domainRole: runtimeConfig.DomainRole, queueRole: runtimeConfig.QueueRole, riverSchema: runtimeConfig.RiverSchema,
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

func (database *postgresReconcilerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

type reconcilerDependencySources struct {
	openDatabase        func(context.Context, config.Config) (reconcilerDatabase, error)
	loadRuntimeRegistry func(string) (*jobruntime.Registry, error)
	buildRelay          func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error)
	newLoop             func(joboutbox.RelayStepper, joboutbox.ReconcilerLoopConfig) (*joboutbox.ReconcilerLoop, error)
	contractRoot        string

	loadSyncDispatchRegistry func(string) (*syncdispatchcontract.Registry, error)
	buildSyncRouteFence      func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncroute.Checker, error)
	buildSyncShadow          func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error)
	buildSyncMutation        func(*pgxpool.Pool, *pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error)
	newSyncRecorder          func(*slog.Logger) (reconcilerObservationRecorder, error)
	newSyncLoop              func(syncreconciler.Stepper, syncreconciler.LoopConfig) (*syncreconciler.Loop, error)
	syncDispatchContractRoot string
}

// reconcilerActivation is a source-reviewed composition seam. It is
// deliberately not configurable through environment or deployment profiles:
// changing from observation to mutation must retain concrete River delivery
// capabilities in the same reviewed source change.
type reconcilerActivation struct {
	syncMutation bool
}

var checkedInReconcilerActivation = reconcilerActivation{}

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

func buildSyncMutationPipeline(
	domainPool *pgxpool.Pool,
	queuePool *pgxpool.Pool,
	registry *syncdispatchcontract.Registry,
) (syncreconciler.Stepper, error) {
	repair, err := syncreconciler.NewLeaseRepair(domainPool)
	if err != nil {
		return nil, err
	}
	materializer, err := syncreconciler.NewMaterializer(domainPool)
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
	// The checked-in contract is Celery-only, so nil delivery callbacks are
	// valid and the mutation kernel performs no transport transaction. A later
	// River route fails closed until that same source-reviewed composition binds
	// its concrete publisher.
	return syncreconciler.NewMutationPipeline(
		repair,
		materializer,
		kernel,
		observer,
		nil,
		nil,
		syncreconciler.DefaultMutationPipelineConfig(),
	)
}

// reconcilerObservationRecorder is the command-owned recorder seam. The
// sync observer loop only offers observations; this command owns lifecycle
// shutdown so its worker cannot outlive the database pools it observes.
type reconcilerObservationRecorder interface {
	syncreconciler.ObservationRecorder
	Shutdown(context.Context) error
}

func buildReconcilerRelay(
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
	routes, err := jobroute.NewController(domainPool, registry, quiescer)
	if err != nil {
		return nil, err
	}
	return joboutbox.NewRelayWithRoutes(repository, inserter, routes, joboutbox.DefaultRelayConfig())
}

type reconcilerDependencies struct {
	database    reconcilerDatabase
	databaseErr error

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
		return nil, errReconcilerDependencyUnavailable
	}

	dependencies := buildReconcilerDependencies(ctx, cfg, registry, logger, activation, sources)
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "job_registry", check: dependencies.registryReady},
		{name: "queue_postgres", check: dependencies.queueReady},
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
	dependencies := &reconcilerDependencies{}
	if sources.openDatabase == nil {
		dependencies.databaseErr = errReconcilerDependencyUnavailable
	} else {
		dependencies.database, dependencies.databaseErr = sources.openDatabase(ctx, cfg)
		if dependencies.databaseErr != nil {
			dependencies.databaseErr = errReconcilerDependencyUnavailable
			dependencies.disableDatabase()
		}
	}
	if sources.loadRuntimeRegistry == nil || sources.contractRoot == "" {
		dependencies.registryErr = errReconcilerDependencyUnavailable
	} else {
		dependencies.runtimeRegistry, dependencies.registryErr = sources.loadRuntimeRegistry(sources.contractRoot)
	}
	if sources.loadSyncDispatchRegistry == nil || sources.syncDispatchContractRoot == "" {
		dependencies.syncRegistryErr = errReconcilerDependencyUnavailable
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
		dependencies.relayErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}

	relay, err := sources.buildRelay(
		dependencies.database.DomainPool(),
		dependencies.database.QueuePool(),
		cfg.RiverDatabaseSchema,
		dependencies.runtimeRegistry,
	)
	if err != nil || relay == nil {
		dependencies.relayErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	loop, err := sources.newLoop(relay, joboutbox.DefaultReconcilerLoopConfig(registry))
	if err != nil || loop == nil {
		dependencies.loopErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	dependencies.loop = loop
	routeFence, err := sources.buildSyncRouteFence(dependencies.database.DomainPool(), dependencies.syncDispatchRegistry)
	if err != nil || routeFence == nil {
		dependencies.syncRouteFenceErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	dependencies.syncRouteFence = routeFence
	var syncStepper syncreconciler.Stepper
	if activation.syncMutation {
		syncStepper, err = sources.buildSyncMutation(
			dependencies.database.DomainPool(),
			dependencies.database.QueuePool(),
			dependencies.syncDispatchRegistry,
		)
	} else {
		syncStepper, err = sources.buildSyncShadow(
			dependencies.database.DomainPool(),
			dependencies.syncDispatchRegistry,
		)
	}
	if err != nil || syncStepper == nil {
		dependencies.syncObserverErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	if logger == nil {
		dependencies.syncRecorderErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	recorder, err := sources.newSyncRecorder(logger)
	dependencies.syncRecorder = recorder
	if err != nil || recorder == nil {
		dependencies.syncRecorderErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}
	syncLoopConfig := syncreconciler.DefaultLoopConfig(registry)
	syncLoopConfig.Recorder = recorder
	syncLoop, err := sources.newSyncLoop(syncStepper, syncLoopConfig)
	if err != nil || syncLoop == nil {
		dependencies.syncLoopErr = errReconcilerDependencyUnavailable
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
		dependencies.databaseErr = errReconcilerDependencyUnavailable
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
