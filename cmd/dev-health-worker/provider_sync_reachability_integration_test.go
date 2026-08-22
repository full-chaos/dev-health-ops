//go:build integration

package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/riverqueue/river"
)

// TestBuildProviderSyncWorkerConstructsRealDependenciesForTheSelectedQueue
// proves buildProviderSyncWorker wires a real, live-dependency family
// (ClickHouse, Valkey, Postgres) once the process selects the provider-unit
// queue. CHAOS-4054 removed every per-route WORKER_*_ENABLED switch: capability
// is always on, so there is no longer a route-by-route matrix to iterate --
// the "default" case exercises every RouteReady pair that needs no extra
// config, and "github_work_items" exercises the one family that still reads
// explicit config (the shared status-mapping/investment-config artifact
// paths).
func TestBuildProviderSyncWorkerConstructsRealDependenciesForTheSelectedQueue(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, configure := range map[string]func(*config.Config){
		"default": func(*config.Config) {},
		"github_work_items": func(cfg *config.Config) {
			cfg.WorkerGithubWorkItemsStatusMappingPath = filepath.Join(
				"src", "dev_health_ops", "config", "status_mapping.yaml",
			)
			cfg.WorkerGithubWorkItemsInvestmentConfigPath = filepath.Join(
				"src", "dev_health_ops", "config", "investment_areas.yaml",
			)
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config.Config{
				Queues:                 []string{"sync", "sync_provider"},
				WorkerQueueConcurrency: map[string]int{"sync": 3, "sync_provider": 9},
				RiverDatabaseSchema:    "river",
				SettingsEncryptionKey:  secrets.NewValue("test-encryption-key"),
				ClickHouseURI:          secrets.NewValue(clickhouse.URI),
				ValkeyURI:              secrets.NewValue(valkey.URI),
			}
			configure(&cfg)
			family, err := buildProviderSyncWorker(
				ctx, cfg, reportBuilderDatabase(t), registry, collector, slog.Default(), river.NewWorkers(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(family.handlers) != 1 || len(family.queues) != 1 || family.queues[0].MaxWorkers != 9 {
				t.Fatalf("provider sync family=%#v", family)
			}
			if err := closeWorkerFamily(family); err != nil {
				t.Fatal(err)
			}
		})
	}
}
