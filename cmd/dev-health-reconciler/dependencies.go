package main

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultReconcilerContractRoot = "contracts/jobs/v1"

var errReconcilerDependencyUnavailable = errors.New("reconciler readiness dependency is unavailable")

// reconcilerDatabase keeps the command's domain and queue-control trust
// boundaries testable without weakening the production RuntimePools contract.
type reconcilerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
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

func (database *postgresReconcilerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

type reconcilerDependencySources struct {
	openDatabase        func(context.Context, config.Config) (reconcilerDatabase, error)
	loadRuntimeRegistry func(string) (*jobruntime.Registry, error)
	buildRelay          func(*pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error)
	newLoop             func(joboutbox.RelayStepper, joboutbox.ReconcilerLoopConfig) (*joboutbox.ReconcilerLoop, error)
	contractRoot        string
}

var productionReconcilerDependencySources = reconcilerDependencySources{
	openDatabase:        openReconcilerDatabase,
	loadRuntimeRegistry: jobruntime.Load,
	buildRelay:          buildReconcilerRelay,
	newLoop:             joboutbox.NewReconcilerLoop,
	contractRoot:        defaultReconcilerContractRoot,
}

func buildReconcilerRelay(
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
	return joboutbox.NewRelay(repository, inserter, joboutbox.DefaultRelayConfig())
}

type reconcilerDependencies struct {
	database    reconcilerDatabase
	databaseErr error

	runtimeRegistry *jobruntime.Registry
	registryErr     error
	relayErr        error
	loop            *joboutbox.ReconcilerLoop
	loopErr         error
}

func configureReconcilerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources reconcilerDependencySources,
) ([]lifecycle.Component, error) {
	if registry == nil {
		return nil, errReconcilerDependencyUnavailable
	}

	dependencies := buildReconcilerDependencies(ctx, cfg, registry, sources)
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "job_registry", check: dependencies.registryReady},
		{name: "queue_postgres", check: dependencies.queueReady},
		{name: "river_schema", check: dependencies.riverSchemaReady(cfg.RiverDatabaseSchema)},
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
		return nil, nil
	}
	if dependencies.database == nil {
		dependencies.close()
		return nil, errReconcilerDependencyUnavailable
	}
	return []lifecycle.Component{
		reconcilerDatabaseLifecycle{database: dependencies.database},
		dependencies.loop,
	}, nil
}

func buildReconcilerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
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
	if dependencies.databaseErr != nil || dependencies.database == nil ||
		dependencies.registryErr != nil || dependencies.runtimeRegistry == nil ||
		sources.buildRelay == nil || sources.newLoop == nil {
		dependencies.relayErr = errReconcilerDependencyUnavailable
		dependencies.disableDatabase()
		return dependencies
	}

	relay, err := sources.buildRelay(
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

func (dependencies *reconcilerDependencies) close() {
	if dependencies != nil && dependencies.database != nil {
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
