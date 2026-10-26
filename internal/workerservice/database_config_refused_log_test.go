package workerservice

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func TestRefusedDatabaseConfigurationIsLoggedAtStartup(t *testing.T) {
	t.Chdir("../..")
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, nil))
	refusal := refusedEndpointConfig().Validate()
	if refusal == nil {
		t.Fatal("fixture config was not refused")
	}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return nil, refusal }
	registry := health.NewRegistry(readinessTestCheckTimeout)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Queues: []string{"heartbeat"}, WorkerQueueConcurrency: map[string]int{"heartbeat": 1}, RiverDatabaseSchema: "river"},
		registry, sources, logger,
	)
	if err != nil || len(components) != 0 {
		t.Fatalf("components = %d, err = %v; want live and unready", len(components), err)
	}
	line := out.String()
	for _, want := range []string{"level=ERROR", "database configuration refused", "domain endpoint equals the queue-control endpoint"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %q", line, want)
		}
	}
	for _, secret := range []string{"s3cretpw", "pgbouncer.internal", "postgres://"} {
		if strings.Contains(line, secret) {
			t.Fatalf("log leaks %q: %s", secret, line)
		}
	}
}

func refusedEndpointConfig() postgres.RuntimeConfig {
	cfg := postgres.DefaultRuntimeConfig(
		"postgres://domain_role:s3cretpw@pgbouncer.internal/app",
		"postgres://queue_role:s3cretpw@pgbouncer.internal/app",
		"domain_role", "queue_role",
	)
	cfg.DomainTransactionPooler = true
	return cfg
}
