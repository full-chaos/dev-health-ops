package schedulerservice

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

type recordedSchedulerDatabase struct {
	*fakeSchedulerDatabase
	schemagatetest.Database
}

func schemaGateSources(database schedulerDatabase) schedulerRuntimeSources {
	return schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) { return database, nil },
		newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			return nil, errors.New("composition reached")
		},
		newCoordinator: schedulersync.NewOccurrenceCoordinator,
		newLoop:        schedulersync.NewLoop,
		newOccurrences: stubOccurrenceSource,
		newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
			return nil, errors.New("composition reached")
		},
	}
}

func TestSchedulerRefusesToStartOnADatabaseBehindTheBuild(t *testing.T) {
	fake := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
	_, err := buildSchedulerLoopWithSources(
		context.Background(), config.Config{RiverDatabaseSchema: "river"}, health.NewRegistry(time.Second),
		schemaGateSources(recordedSchedulerDatabase{fakeSchedulerDatabase: fake, Database: schemagatetest.OneBehind(t)}),
	)
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "scheduler_schema_behind" {
		t.Fatalf("a database one migration behind: error = %v, want scheduler_schema_behind", err)
	}
	if !fake.closed.Load() {
		t.Fatal("the refused database was not closed")
	}
}

func TestSchedulerComposesOnADatabaseAtOrAheadOfTheBuild(t *testing.T) {
	for name, database := range map[string]schemagatetest.Database{
		"equal": schemagatetest.AtHead(t),
		"ahead": schemagatetest.Ahead(t),
	} {
		fake := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
		_, err := buildSchedulerLoopWithSources(
			context.Background(), config.Config{RiverDatabaseSchema: "river"}, health.NewRegistry(time.Second),
			schemaGateSources(recordedSchedulerDatabase{fakeSchedulerDatabase: fake, Database: database}),
		)
		var coded interface{ DependencyReason() string }
		if errors.As(err, &coded) && coded.DependencyReason() == "scheduler_schema_behind" {
			t.Fatalf("%s: the gate refused a database that is not behind: %v", name, err)
		}
	}
}
