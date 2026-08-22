//go:build integration

package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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
	// CHAOS-4054: there is no "default" case any more. A process that serves
	// the provider-unit queue must be able to serve every RouteReady route,
	// work-items included, so the artifacts are part of a valid provider-sync
	// configuration rather than an opt-in extra. The old default/github_work_items
	// split existed because a route switch could say "this deployment does not
	// serve work-items"; nothing says that now.
	for _, name := range []string{"work_items_artifacts_configured"} {
		t.Run(name, func(t *testing.T) {
			cfg := validGitHubWorkItemsRuntimeConfig(t)
			cfg.Queues = []string{"sync", "sync_provider"}
			cfg.WorkerQueueConcurrency = map[string]int{"sync": 3, "sync_provider": 9}
			cfg.RiverDatabaseSchema = "river"
			cfg.SettingsEncryptionKey = secrets.NewValue("test-encryption-key")
			cfg.ClickHouseURI = secrets.NewValue(clickhouse.URI)
			cfg.ValkeyURI = secrets.NewValue(valkey.URI)
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
