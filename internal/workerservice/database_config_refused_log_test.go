package workerservice

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func TestRefusedDatabaseConfigurationIsLoggedAtStartup(t *testing.T) {
	endpointEqual := refusedEndpointConfig().Validate()
	if endpointEqual == nil {
		t.Fatal("fixture config was not refused")
	}
	planted := fmt.Errorf("cannot parse postgres://leak-user:leak-pass@leak-host.example/db: %w", postgres.ErrInvalidConfig)
	for _, row := range []struct {
		name string
		err  error
		want string
	}{
		{"endpoint equals queue", endpointEqual, "reason=queue_control_transaction_mode"},
		{"dsn missing", postgres.ErrDomainDatabaseRequired, "reason=domain_dsn_missing"},
		{"planted dsn text", planted, "reason=invalid_config"},
	} {
		t.Run(row.name, func(t *testing.T) {
			var out bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&out, nil))
			sources := productionWorkerDependencySources
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return nil, row.err }
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
			for _, want := range []string{"level=ERROR", "database configuration refused", row.want} {
				if !strings.Contains(line, want) {
					t.Fatalf("log %q lacks %q", line, want)
				}
			}
			for _, secret := range []string{"s3cretpw", "pgbouncer.internal", "postgres://", "leak-user", "leak-pass", "leak-host"} {
				if strings.Contains(line, secret) {
					t.Fatalf("log leaks %q: %s", secret, line)
				}
			}
		})
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
