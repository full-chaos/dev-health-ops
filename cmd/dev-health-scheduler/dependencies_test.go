package main

import (
	"context"
	"errors"
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

func TestSchedulerProductionFactoryBuildsReviewedRuntime(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}}
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
		newLoop: schedulersync.NewLoop,
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
		},
	)
	if !errors.Is(err, errSchedulerActivationUnavailable) || !database.closed.Load() {
		t.Fatalf("err=%v database_closed=%v", err, database.closed.Load())
	}
}
