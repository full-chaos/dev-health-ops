package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// promotedContractRoot copies the checked-in contract tree and routes exactly
// the named kinds to River. Startup validation is scoped to executable kinds,
// so a fixture that promotes nothing would let every assertion pass vacuously.
func promotedContractRoot(t *testing.T, kinds ...string) (*jobruntime.Registry, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "v1")
	if err := os.CopyFS(root, os.DirFS(defaultContractRoot)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "migration-state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		SchemaVersion int              `json:"schema_version"`
		Jobs          []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	for _, job := range document.Jobs {
		kind, _ := job["kind"].(string)
		if slices.Contains(kinds, kind) {
			job["state"] = "go_default"
			job["route"] = "river"
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := jobruntime.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	return registry, root
}

func reportBuilderDatabase(t *testing.T) *postgresWorkerDatabase {
	t.Helper()
	ctx := context.Background()
	domainPool, err := pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domainPool.Close)
	queuePool, err := pgxpool.New(ctx, "postgresql://queue@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queuePool.Close)
	return &postgresWorkerDatabase{
		pools: &postgres.RuntimePools{Domain: domainPool, QueueControl: queuePool},
	}
}

func TestReportBuilderStaysDormantWhileRoutesAreCelery(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	family, err := buildReportWorker(
		context.Background(),
		config.Config{Profile: "heavy", RiverDatabaseSchema: "river"},
		reportBuilderDatabase(t),
		registry,
		reportTestObserver(t),
		slog.Default(),
	)
	if err != nil {
		t.Fatalf("dormant report builder error = %v", err)
	}
	if family.component != nil || len(family.handlers) != 0 || len(family.queues) != 0 {
		t.Fatalf("celery-routed report kinds constructed a runtime: %#v", family)
	}
}

func TestReportBuilderRefusesPartialRoutePromotion(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry, _ := promotedContractRoot(t, jobcontract.KindReportExecuteOnDemand)
	_, err := buildReportWorker(
		context.Background(),
		config.Config{
			Profile:             "heavy",
			RiverDatabaseSchema: "river",
			ClickHouseURI:       secrets.NewValue("clickhouse://127.0.0.1:1/default"),
		},
		reportBuilderDatabase(t),
		registry,
		reportTestObserver(t),
		slog.Default(),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("half-promoted report pair error = %v, want unavailable", err)
	}
}

// TestReportBuilderRequiresClickHouse proves the report runtime cannot be
// half-constructed: without its query backend the heavy profile closes rather
// than registering adapters that would fail every fetch.
func TestReportBuilderRequiresClickHouse(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry, _ := promotedContractRoot(t,
		jobcontract.KindReportExecuteOnDemand,
		jobcontract.KindReportExecuteScheduled,
	)
	_, err := buildReportWorker(
		context.Background(),
		config.Config{Profile: "heavy", RiverDatabaseSchema: "river"},
		reportBuilderDatabase(t),
		registry,
		reportTestObserver(t),
		slog.Default(),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("missing ClickHouse error = %v, want unavailable", err)
	}
}

// TestReportQueueBudgetMatchesDeploymentManifest keeps the constructed River
// budget and the reviewed capacity plan from drifting apart silently.
func TestReportQueueBudgetMatchesDeploymentManifest(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	contracts, err := jobcontract.LoadRegistry(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _, err := deploymentcontract.Load(defaultDeploymentProfile, contracts)
	if err != nil {
		t.Fatal(err)
	}
	process, ok := riverProcessForProfile(manifest, "heavy")
	if !ok {
		t.Fatal("heavy process is missing from the deployment manifest")
	}
	for _, queue := range process.QueueWorkers {
		if queue.Queue != reportsQueue {
			continue
		}
		if queue.MaxWorkers != reportsQueueWorkers {
			t.Fatalf("reports queue budget = %d, constructed %d", queue.MaxWorkers, reportsQueueWorkers)
		}
		return
	}
	t.Fatal("deployment manifest does not budget the reports queue")
}

func reportTestObserver(t *testing.T) jobruntime.Observer {
	t.Helper()
	collector, err := jobruntime.NewMetricsCollector(
		jobruntime.MetricDimensions{Profiles: []string{"heavy"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return collector
}
