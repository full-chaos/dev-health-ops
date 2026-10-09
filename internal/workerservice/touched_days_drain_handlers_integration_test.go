//go:build integration

package workerservice

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The worker family that buildDailyWorker builds triggers the drain from its
// two handlers: the dispatcher (the nightly floor) and the finalize handler
// (the continuation). The test reads the handlers the builder registered, so a
// builder that registers a handler without the drain fails here.
func TestDailyHandlersOfTheWorkerTriggerTheTouchedDaysDrain(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	domain, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domain.Close)
	queue, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	database := &postgresWorkerDatabase{
		pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queue},
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

	family, err := buildDailyWorker(
		cfg, database, registry, collector, slog.New(slog.NewTextHandler(io.Discard, nil)), river.NewWorkers())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, cleanup := range family.cleanups {
			_ = cleanup()
		}
	})

	registered := map[string]bool{}
	for _, spec := range family.handlers {
		registered[spec.Kind] = true
	}
	for _, kind := range []string{jobcontract.KindDailyMetricsDispatch, jobcontract.KindDailyMetricsFinalize} {
		if !registered[kind] {
			t.Fatalf("the worker family did not register %s: the test measured nothing", kind)
		}
	}
	triggers := family.dailyDrainTriggers
	if triggers.dispatcher == nil || triggers.finalize == nil {
		t.Fatalf("the worker family names no dispatcher or no finalize handler: %+v", triggers)
	}
	var floor, continuation daily.TouchedDaysDrainer = triggers.dispatcher.TouchedDaysDrainer(), triggers.finalize.TouchedDaysDrainer()
	if floor == nil {
		t.Fatal("the dispatcher of the worker has no drain: the nightly run would trigger no pass")
	}
	if continuation == nil {
		t.Fatal("the finalize handler of the worker has no drain: the end of a run would trigger no pass")
	}
	if floor != continuation {
		t.Fatal("the dispatcher and the finalize handler of the worker have two drains, want one")
	}

	// The constructors the builder uses refuse a nil drain.
	if _, err := newDrainingDailyDispatcher(nil, nil, nil, nil); err == nil {
		t.Fatal("a dispatcher without a drain was built")
	}
	if _, err := newDrainingDailyFinalizeHandler(nil, nil, nil); err == nil {
		t.Fatal("a finalize handler without a drain was built")
	}

	// The finalize handler of the worker also carries the run-level retraction
	// of stale team keys: without it a run ends with the keys that its
	// partitions no longer produce in place, and nothing else supersedes them.
	if !triggers.finalize.HasStaleKeyRetractor() {
		t.Fatal("the finalize handler of the worker has no stale-key retractor: a recomputed day would be counted under two team ids")
	}
	if _, err := newDrainingDailyFinalizeHandler(nil, floor, nil); err == nil {
		t.Fatal("a finalize handler with no connection for the stale-key retractor was built")
	}
}
