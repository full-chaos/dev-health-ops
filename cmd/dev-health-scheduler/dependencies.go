package main

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/jackc/pgx/v5/pgxpool"
)

type schedulerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	DomainPool() *pgxpool.Pool
	Close()
}

type postgresSchedulerDatabase struct {
	pools       *postgres.RuntimePools
	domainRole  string
	queueRole   string
	riverSchema string
}

func openSchedulerDatabase(ctx context.Context, cfg config.Config) (schedulerDatabase, error) {
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg)
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresSchedulerDatabase{
		pools:       pools,
		domainRole:  runtimeConfig.DomainRole,
		queueRole:   runtimeConfig.QueueRole,
		riverSchema: runtimeConfig.RiverSchema,
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

func (database *postgresSchedulerDatabase) DomainPool() *pgxpool.Pool {
	if database == nil || database.pools == nil {
		return nil
	}
	return database.pools.Domain
}

func (database *postgresSchedulerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
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
	// processes must never both believe they own periodic work.
	newFixedLoop func(*pgxpool.Pool, *health.Registry) (lifecycle.Component, error)
}

var productionSchedulerRuntimeSources = schedulerRuntimeSources{
	openDatabase: openSchedulerDatabase,
	newRepository: func(pool *pgxpool.Pool) (schedulersync.HandoffStepper, error) {
		return schedulersync.NewMutationRepository(pool)
	},
	newCoordinator: schedulersync.NewOccurrenceCoordinator,
	newLoop:        schedulersync.NewLoop,
	newFixedLoop:   buildFixedScheduleLoop,
}

func buildProductionSchedulerLoop(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) (lifecycle.Component, error) {
	return buildSchedulerLoopWithSources(
		ctx,
		cfg,
		registry,
		productionSchedulerRuntimeSources,
	)
}

func buildSchedulerLoopWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources schedulerRuntimeSources,
) (lifecycle.Component, error) {
	if ctx == nil || registry == nil || sources.openDatabase == nil ||
		sources.newRepository == nil || sources.newCoordinator == nil ||
		sources.newLoop == nil || sources.newFixedLoop == nil {
		return nil, errSchedulerActivationUnavailable
	}
	database, err := sources.openDatabase(ctx, cfg)
	if err != nil || database == nil {
		return nil, errSchedulerActivationUnavailable
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
	if err := registry.RegisterRequired(
		"river_schema",
		func(ctx context.Context) error {
			return database.RiverSchemaReady(ctx, cfg.RiverDatabaseSchema)
		},
	); err != nil {
		return nil, err
	}
	repository, err := sources.newRepository(database.DomainPool())
	if err != nil || repository == nil {
		return nil, errSchedulerActivationUnavailable
	}
	coordinator := sources.newCoordinator()
	if coordinator == nil {
		return nil, errSchedulerActivationUnavailable
	}
	loop, err := sources.newLoop(
		repository,
		coordinator,
		schedulersync.DefaultLoopConfig(registry),
	)
	if err != nil || loop == nil {
		return nil, errSchedulerActivationUnavailable
	}
	fixedLoop, err := sources.newFixedLoop(database.DomainPool(), registry)
	if err != nil || fixedLoop == nil {
		return nil, errSchedulerActivationUnavailable
	}
	closeOnError = false
	return schedulerRuntime{database: database, loop: loop, fixedLoop: fixedLoop}, nil
}

type schedulerRuntime struct {
	database  schedulerDatabase
	loop      *schedulersync.Loop
	fixedLoop lifecycle.Component
}

func (schedulerRuntime) Name() string { return "sync-scheduler-runtime" }

// Start brings up the product schedule loop first. If it cannot start, the
// fixed maintenance loop is never started either: a process that owns half the
// periodic surface is harder to reason about than one that stays closed.
func (component schedulerRuntime) Start(ctx context.Context) error {
	if component.database == nil || component.loop == nil || component.fixedLoop == nil {
		return errSchedulerActivationUnavailable
	}
	if err := component.loop.Start(ctx); err != nil {
		return err
	}
	if err := component.fixedLoop.Start(ctx); err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = component.loop.Shutdown(shutdownCtx)
		return err
	}
	return nil
}

// Shutdown stops both loops and always closes the database, even when one loop
// fails to stop cleanly.
func (component schedulerRuntime) Shutdown(ctx context.Context) error {
	if component.database != nil {
		defer component.database.Close()
	}
	if component.loop == nil || component.fixedLoop == nil {
		return errSchedulerActivationUnavailable
	}
	fixedErr := component.fixedLoop.Shutdown(ctx)
	syncErr := component.loop.Shutdown(ctx)
	return errors.Join(fixedErr, syncErr)
}
