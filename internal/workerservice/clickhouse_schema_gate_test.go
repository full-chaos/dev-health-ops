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

func clickHouseGateSources(t *testing.T, fake *fakeWorkerDatabase, check func(context.Context, config.Config) error) workerDependencySources {
	sources := schemaGateSources(schemagatetest.AtHead(t), fake)
	sources.checkClickHouseSchema = check
	return sources
}

func TestWorkerRefusesToStartOnAClickHouseSchemaBehindTheBuild(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	fake := &fakeWorkerDatabase{}
	components, err := configureWorkerDependenciesWithSources(
		context.Background(), schemaGateConfig(), health.NewRegistry(readinessTestCheckTimeout),
		clickHouseGateSources(t, fake, func(context.Context, config.Config) error { return errors.New("behind") }),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) || len(components) != 0 {
		t.Fatalf("error = %v, %d components; want a refusal", err, len(components))
	}
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "worker_clickhouse_schema_behind" {
		t.Fatalf("reason code = %v, want worker_clickhouse_schema_behind", err)
	}
	if !fake.closed.Load() {
		t.Fatal("the refused database was not closed")
	}
}

func TestWorkerStartsWhenTheClickHouseGatePasses(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	dependencies := buildWorkerDependencies(context.Background(), schemaGateConfig(),
		clickHouseGateSources(t, &fakeWorkerDatabase{}, func(context.Context, config.Config) error { return nil }))
	if dependencies.databaseErr != nil || dependencies.database == nil {
		t.Fatalf("database = %v, error = %v; want it kept", dependencies.database, dependencies.databaseErr)
	}
	dependencies.close()
}

func TestEnforceClickHouseSchemaSkipsWhenClickHouseIsNotConfigured(t *testing.T) {
	if err := enforceClickHouseSchema(context.Background(), config.Config{}); err != nil {
		t.Fatalf("got %v", err)
	}
}
