package workerservice

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type recordedWorkerDatabase struct {
	*fakeWorkerDatabase
	schemagatetest.Database
}

func schemaGateSources(database schemagatetest.Database, fake *fakeWorkerDatabase) workerDependencySources {
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return recordedWorkerDatabase{fakeWorkerDatabase: fake, Database: database}, nil
	}
	return sources
}

func schemaGateConfig() config.Config {
	return config.Config{
		Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
		WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
		RiverDatabaseSchema:    "river",
	}
}

func TestWorkerRefusesToStartOnADatabaseBehindTheBuild(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	fake := &fakeWorkerDatabase{}
	components, err := configureWorkerDependenciesWithSources(
		context.Background(), schemaGateConfig(), health.NewRegistry(readinessTestCheckTimeout),
		schemaGateSources(schemagatetest.OneBehind(t), fake),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) || len(components) != 0 {
		t.Fatalf("a database one migration behind: error = %v, %d components; want a refusal", err, len(components))
	}
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "worker_schema_behind" {
		t.Fatalf("reason code = %v, want worker_schema_behind", err)
	}
	if !fake.closed.Load() {
		t.Fatal("the refused database was not closed")
	}
}

func TestWorkerStartsOnADatabaseAtOrAheadOfTheBuild(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for name, database := range map[string]schemagatetest.Database{
		"equal": schemagatetest.AtHead(t),
		"ahead": schemagatetest.Ahead(t),
	} {
		dependencies := buildWorkerDependencies(context.Background(), schemaGateConfig(), schemaGateSources(database, &fakeWorkerDatabase{}))
		if dependencies.databaseErr != nil || dependencies.database == nil {
			t.Fatalf("%s: database = %v, error = %v; want it kept", name, dependencies.database, dependencies.databaseErr)
		}
		dependencies.close()
	}
}
