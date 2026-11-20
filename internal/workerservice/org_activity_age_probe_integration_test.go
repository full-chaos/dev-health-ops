//go:build integration

package workerservice

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/orgactivity/refresh"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The finalize case of buildDailyWorker registers the org activity age probe on pools.Domain and the global meter. This test builds the
// real worker family and reads the gauge through the global meter. The SQL clock is pinned: the domain pool resolves now() to a
// test-owned public.now() (search_path public, pg_catalog), so the age is an exact value of the seeded timestamps, not of wall time.
func TestOrgActivityAgeProbeOfTheWorkerReportsTheNewestRowAgeAndNoSampleWhenEmpty(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)

	clickhouse := startLegacyClickHouse(t, ctx)
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "")
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	prepareMultiReplicaDatabase(t, ctx, admin)
	if _, err := admin.Exec(ctx, `CREATE FUNCTION public.now() RETURNS timestamptz LANGUAGE sql STABLE
AS $$ SELECT timestamptz '2026-11-20 12:00:00+00' $$`); err != nil {
		t.Fatal(err)
	}

	domainConfig, err := pgxpool.ParseConfig(postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainConfig.ConnConfig.RuntimeParams["search_path"] = "public, pg_catalog"
	domain, err := pgxpool.NewWithConfig(ctx, domainConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	queue, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	var pinned time.Time
	if err := domain.QueryRow(ctx, `SELECT now()`).Scan(&pinned); err != nil || !pinned.Equal(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("the domain pool did not resolve now() to the pinned clock: %v %v", pinned, err)
	}

	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Service:                  "dev-health-worker",
		Queues:                   []string{metricsQueue},
		RiverDatabaseSchema:      "river",
		OperationalBridgeTimeout: 20 * time.Second,
		ClickHouseURI:            secrets.NewValue(clickhouse),
		WorkerRemainingComplexityConfigPath: filepath.Join(
			"src", "dev_health_ops", "config", "complexity.yaml",
		),
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queue}}

	// build wires a fresh worker family against a fresh global meter and returns the gauge samples and the failure-counter samples of one collection.
	build := func(t *testing.T) (gauge []float64, failed []int64) {
		t.Helper()
		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		previous := otel.GetMeterProvider()
		otel.SetMeterProvider(provider)
		t.Cleanup(func() {
			otel.SetMeterProvider(previous)
			_ = provider.Shutdown(context.Background())
		})
		family, err := buildDailyWorker(cfg, database, registry, collector, slog.New(slog.NewTextHandler(io.Discard, nil)), river.NewWorkers())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			for _, cleanup := range family.cleanups {
				_ = cleanup()
			}
		})
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			t.Fatal(err)
		}
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				switch data := m.Data.(type) {
				case metricdata.Gauge[float64]:
					if m.Name == refresh.AgeGaugeName {
						for _, p := range data.DataPoints {
							gauge = append(gauge, p.Value)
						}
					}
				case metricdata.Sum[int64]:
					if m.Name == refresh.AgeReadFailedCounterName {
						for _, p := range data.DataPoints {
							failed = append(failed, p.Value)
						}
					}
				}
			}
		}
		return gauge, failed
	}

	t.Run("an empty table records no sample at all", func(t *testing.T) {
		gauge, failed := build(t)
		if len(gauge) != 0 {
			t.Errorf("gauge = %v, want no data point for an empty org_activity (not 0)", gauge)
		}
		if len(failed) != 1 || failed[0] != 0 {
			t.Errorf("read-failure counter = %v, want one series at 0: the probe was not wired or the read failed", failed)
		}
	})

	t.Run("the gauge is the age of the newest row over all orgs", func(t *testing.T) {
		clock := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
		// One row per org (org_id is the key): refreshed 3 h, 1 h and 30 min ago (the newest, held by the last org); computed_at is 10 h older, so it is not the age column.
		for index, age := range []time.Duration{3 * time.Hour, time.Hour, 30 * time.Minute} {
			id := uuid.New()
			if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "age-probe-"+string(rune('a'+index))); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(ctx, `INSERT INTO public.org_activity (org_id, has_data, computed_at, refreshed_at) VALUES ($1, true, $2, $3)`,
				id, clock.Add(-age-10*time.Hour), clock.Add(-age)); err != nil {
				t.Fatal(err)
			}
		}
		gauge, failed := build(t)
		if len(gauge) != 1 || gauge[0] != 1800 {
			t.Errorf("gauge = %v, want exactly [1800] (now - max(refreshed_at) of the 30 min old row)", gauge)
		}
		if len(failed) != 1 || failed[0] != 0 {
			t.Errorf("read-failure counter = %v, want one series at 0", failed)
		}
	})
}
