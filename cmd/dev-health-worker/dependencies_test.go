package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

func TestWorkerSpecConfiguresDependencies(t *testing.T) {
	if workerSpec.Service != "dev-health-worker" || workerSpec.DefaultProfile != "" {
		t.Fatalf("unexpected worker spec: %#v", workerSpec)
	}
	if len(workerSpec.Profiles) != 0 || !workerSpec.RequireQueues {
		t.Fatalf("worker must require queues and reject profile selection: %#v", workerSpec)
	}
	if workerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal("worker dependency configuration is not wired")
	}
}

func TestDeploymentSuppliesQueueConcurrencyAndObservableBudget(t *testing.T) {
	t.Parallel()
	budgets := selectedQueueBudgets(
		[]string{"heartbeat", "webhooks"},
		[]string{"coverage", "heartbeat", "webhooks"},
		map[string]int{"coverage": 99, "heartbeat": 7, "webhooks": 13},
	)
	want := []jobruntime.QueueBudget{
		{Queue: "heartbeat", MaxWorkers: 7},
		{Queue: "webhooks", MaxWorkers: 13},
	}
	if !slices.Equal(budgets, want) {
		t.Fatalf("selected queue budgets = %#v, want %#v", budgets, want)
	}
	if got := formatQueueBudgets(budgets); got != "heartbeat=7,webhooks=13" {
		t.Fatalf("formatted queue budgets = %q", got)
	}
}

func TestWorkerGroupIsAnObservableLabelNotAQueueSelector(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return &fakeWorkerDatabase{}, nil
	}
	dependencies := buildWorkerDependencies(context.Background(), config.Config{
		Queues:                 []string{"heartbeat"},
		WorkerQueueConcurrency: map[string]int{"heartbeat": 7},
		WorkerGroup:            "my-latency-pool",
	}, sources)
	defer dependencies.close()
	if dependencies.startupErr != nil {
		t.Fatal(dependencies.startupErr)
	}
	if dependencies.workerGroup != "my-latency-pool" ||
		!slices.Equal(dependencies.startup.SelectedQueues, []string{"heartbeat"}) ||
		!slices.Equal(dependencies.startup.ConfiguredQueues, []jobruntime.QueueBudget{{Queue: "heartbeat", MaxWorkers: 7}}) {
		t.Fatalf("worker dependencies = %#v", dependencies)
	}
}

func TestNoDatabaseConfigurationStaysLiveAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependencies(
		context.Background(),
		config.Config{
			Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
		},
		registry,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no pool lifecycle without DSNs", len(components))
	}
	if registry.RequiredCount() != 9 {
		t.Fatalf("required checks = %d, want 9", registry.RequiredCount())
	}
	// Every accepted queue set now owns registered kinds, so queue telemetry is
	// always required and a worker without a database cannot serve a complete
	// scrape. Partial metrics would understate a backlog, so the scrape fails
	// closed instead.
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("write worker metrics error = %v, want stable unavailable error", err)
	}
	if metrics.Len() != 0 {
		t.Fatalf("failed scrape emitted partial metrics:\n%s", metrics.String())
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	status := registry.Readiness(context.Background())
	want := []string{
		"domain_postgres",
		"queue_completeness",
		"queue_postgres",
		"queued_contract_versions",
		"river_schema",
	}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

// TestProviderRouteSwitchesReadyFollowsQueueTopology proves
// providerRouteSwitchesReady's CHAOS-4054 contract: capability is always on
// in the binary, so the only remaining topology question is whether this
// process selected the provider-unit queue at all.
//
//   - queue not selected: the check always passes, regardless of the
//     work-item runtime config or whether the provider-sync runtime was ever
//     constructed.
//   - queue selected: the check passes only if the work-item runtime config
//     is valid AND the provider-sync runtime was constructed. "Valid" has no
//     absent case any more: capability is always on, so a process that serves
//     the provider-unit queue must be able to serve the work-item family, and
//     unreadable or unset artifacts fail readiness rather than deferring the
//     problem to the first claim.
//
// Every one of the ~40 deleted WORKER_*_ENABLED switches used to gate one
// route's slice of this same check (github/repo-metadata "on" and
// launchdarkly "on" reached the identical branch); since capability is now
// unconditional, none of them distinguish any behavior here, so this test
// asserts the contract directly instead of repeating it once per deleted
// switch.
func TestProviderRouteSwitchesReadyFollowsQueueTopology(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")

	// Queue not selected: always ready, even with an invalid work-item
	// runtime config and no constructed runtime.
	invalidWorkItems := config.Config{
		WorkerGithubWorkItemsStatusMappingPath: "/does/not/exist.yaml",
	}
	if err := providerRouteSwitchesReady(invalidWorkItems, new(bool))(context.Background()); err != nil {
		t.Fatalf("queue not selected: error = %v, want nil", err)
	}

	// Queue selected, runtime never constructed: refused.
	notConstructed := config.Config{Queues: []string{providerUnitQueue}}
	if err := providerRouteSwitchesReady(notConstructed, new(bool))(context.Background()); err == nil {
		t.Fatal("queue selected without a constructed runtime: want error")
	}

	// Queue selected, runtime constructed, no work-item artifacts at all:
	// refused. There is no route switch left that could mean "this deployment
	// opted out of work-items", so unset artifacts are a serving-plane fault
	// an operator must see at startup, not a per-claim surprise.
	constructed := true
	unconfigured := config.Config{Queues: []string{providerUnitQueue}}
	if err := providerRouteSwitchesReady(unconfigured, &constructed)(context.Background()); err == nil {
		t.Fatal("queue selected with no work-item runtime artifacts: want error")
	}

	// Queue selected, runtime constructed, but the work-item runtime config
	// is present and invalid: refused.
	invalidWorkItems.Queues = []string{providerUnitQueue}
	if err := providerRouteSwitchesReady(invalidWorkItems, &constructed)(context.Background()); err == nil {
		t.Fatal("queue selected with an invalid work-item runtime config: want error")
	}

	// Queue selected, runtime constructed, valid work-item runtime config:
	// ready.
	valid := validGitHubWorkItemsRuntimeConfig(t)
	valid.Queues = []string{providerUnitQueue}
	if err := providerRouteSwitchesReady(valid, &constructed)(context.Background()); err != nil {
		t.Fatalf("queue selected with a valid work-item runtime config: error = %v, want nil", err)
	}
}

func TestGitHubWorkItemsRouteReadinessUsesTheProductionRuntimeConfig(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	valid := validGitHubWorkItemsRuntimeConfig(t)
	for _, test := range []struct {
		name    string
		config  config.Config
		runtime bool
		wantErr bool
	}{
		{name: "complete", config: valid, runtime: true},
		{name: "missing runtime", config: valid, wantErr: true},
		{
			name: "missing explicit status mapping path",
			config: config.Config{
				WorkerGithubWorkItemsInvestmentConfigPath: valid.WorkerGithubWorkItemsInvestmentConfigPath,
			},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.config.Queues = []string{providerUnitQueue}
			err := providerRouteSwitchesReady(test.config, &test.runtime)(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}

	t.Setenv("STATUS_MAPPING_PATH", " ")
	valid.Queues = []string{providerUnitQueue}
	if err := providerRouteSwitchesReady(valid, new(bool))(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("ambient status mapping error=%v want worker dependency unavailable", err)
	}
}

func TestGitHubProjectsV2StartupReadinessWarnsOnlyForOrphanedEnvironment(t *testing.T) {
	t.Setenv("GITHUB_PROJECTS_V2", "")
	database := &fakeWorkerDatabase{}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	check := githubProjectsV2StartupReadiness(database, logger)
	if err := check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if database.projectsV2Queries != 0 || output.Len() != 0 {
		t.Fatalf("empty environment queried=%d log=%q", database.projectsV2Queries, output.String())
	}

	const legacyValue = "acme:3,should-never-be-logged:12"
	t.Setenv("GITHUB_PROJECTS_V2", legacyValue)
	if err := check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if database.projectsV2Queries != 1 || !strings.Contains(output.String(), "GITHUB_PROJECTS_V2") ||
		strings.Contains(output.String(), legacyValue) {
		t.Fatalf("env-only warning queries=%d log=%q", database.projectsV2Queries, output.String())
	}
	// Health probes may rerun, but startup warning volume must stay one per
	// process configuration rather than log-spamming readiness polling.
	if err := check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(output.String(), "GITHUB_PROJECTS_V2"); count != 2 {
		// One occurrence is in the message and one in the structured field.
		t.Fatalf("warning count=%d log=%q", count, output.String())
	}

	durable := &fakeWorkerDatabase{projectsV2Configured: true}
	var durableOutput bytes.Buffer
	if err := githubProjectsV2StartupReadiness(
		durable, slog.New(slog.NewTextHandler(&durableOutput, nil)),
	)(context.Background()); err != nil {
		t.Fatal(err)
	}
	if durable.projectsV2Queries != 1 || durableOutput.Len() != 0 {
		t.Fatalf("durable config queried=%d log=%q", durable.projectsV2Queries, durableOutput.String())
	}

	unavailable := &fakeWorkerDatabase{domainErr: errors.New("domain unavailable")}
	if err := githubProjectsV2StartupReadiness(unavailable, logger)(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) || unavailable.projectsV2Queries != 0 {
		t.Fatalf("domain failure error=%v queries=%d", err, unavailable.projectsV2Queries)
	}
}

func TestLaunchDarklyReadinessRequiresConcreteProviderHandlerRegistration(
	t *testing.T,
) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	// sync.team_autoimport ships at go_default in the checked-in contract
	// too, so the selected queue set is only complete once something reports it.
	// A fake sync-coordinator builder supplies that coverage so this test
	// stays isolated to what it actually exercises: the LaunchDarkly
	// provider-route-switch gate on the provider_sync builder.
	autoimportSpec, ok := runtimeRegistry.Descriptor(jobcontract.KindTeamAutoimport)
	if !ok {
		t.Fatal("sync.team_autoimport descriptor missing")
	}
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
	sources.buildOperational = nil
	sources.buildSyncCoordinator = func(
		context.Context, config.Config, workerDatabase, *jobruntime.Registry,
		jobruntime.Observer, *slog.Logger, *river.Workers,
	) (workerFamily, error) {
		return workerFamily{
			handlers: []jobruntime.HandlerSpec{autoimportSpec},
			queues:   []jobruntime.QueueBudget{{Queue: syncCoordinatorQueue, MaxWorkers: 4}},
		}, nil
	}
	sources.buildProviderSync = func(
		_ context.Context,
		_ config.Config,
		_ workerDatabase,
		_ *jobruntime.Registry,
		_ jobruntime.Observer,
		_ *slog.Logger,
		_ *river.Workers,
	) (workerFamily, error) {
		spec, ok := runtimeRegistry.Descriptor("sync.provider_unit")
		if !ok {
			t.Fatal("sync.provider_unit descriptor missing")
		}
		return workerFamily{
			handlers: []jobruntime.HandlerSpec{spec},
			queues: []jobruntime.QueueBudget{
				{Queue: "sync_provider", MaxWorkers: 2},
			},
		}, nil
	}
	registry := health.NewRegistry(100 * time.Millisecond)
	_, err = configureWorkerDependenciesWithSources(
		context.Background(),
		func() config.Config {
			// The provider-unit queue is selected here, so readiness now
			// requires real work-item artifacts (CHAOS-4054).
			cfg := validGitHubWorkItemsRuntimeConfig(t)
			cfg.Queues = []string{"sync", "sync_provider"}
			cfg.RiverDatabaseSchema = "river"
			cfg.WorkerQueueConcurrency = map[string]int{"sync": 4, "sync_provider": 2}
			cfg.DomainDatabaseMaxConns = 4
			cfg.QueueDatabaseMaxConns = 2
			return cfg
		}(),
		registry, sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("registered provider runtime readiness=%#v", status)
	}

	sources.buildProviderSync = nil
	// Drop the sync-coordinator fake too: this half isolates the
	// provider-route queue-topology gate itself, and partial handler
	// coverage (team_autoimport alone) would instead fail closed at
	// construction time -- the invariant TestNestedRiverClientCapabilityIsValidated
	// already proves -- before the readiness gate this assertion inspects
	// ever opens.
	sources.buildSyncCoordinator = nil
	missingRegistry := health.NewRegistry(100 * time.Millisecond)
	_, err = configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues: []string{"sync", "sync_provider"}, RiverDatabaseSchema: "river",
			WorkerQueueConcurrency: map[string]int{"sync": 4, "sync_provider": 2},
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		missingRegistry, sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: missingRegistry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	missing := missingRegistry.Readiness(context.Background())
	if !slices.Contains(missing.Failed, "provider_route_switches") ||
		!slices.Contains(missing.Failed, "queue_completeness") {
		t.Fatalf("missing provider runtime readiness=%#v", missing)
	}
}

func TestTransactionModeQueueControlHasActionableReadinessCategory(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependencies(
		context.Background(),
		config.Config{
			Service:                 "dev-health-worker",
			Queues:                  []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency:  map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			DomainDatabaseURI:       secrets.NewValue("postgresql://domain_role:secret@pgbouncer/app"),
			QueueDatabaseURI:        secrets.NewValue("postgresql://queue_role:secret@pgbouncer/app"),
			QueueDatabaseMode:       config.QueueControlTransaction,
			RiverDatabaseSchema:     "river",
			DomainDatabaseMaxConns:  4,
			QueueDatabaseMaxConns:   2,
			DomainTransactionPooler: true,
		},
		registry,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no pools for rejected queue-control mode", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	want := []string{
		"domain_postgres",
		"queue_completeness",
		"queue_control_config",
		"queue_postgres",
		"queued_contract_versions",
		"river_schema",
	}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want sanitized failures %v", status, want)
	}
}

func TestSelectedQueuesUseRegistryBoundedJobDimensions(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "heartbeat", Kind: "system.heartbeat", Available: 3},
				{Queue: "retention", Kind: "system.retention_cleanup", Available: 2},
			},
			Queues: []riverstore.QueueAgeTelemetry{
				{Queue: "heartbeat", OldestAvailableAge: 12 * time.Second},
				{Queue: "retention", OldestAvailableAge: 4 * time.Second},
			},
			QueueCapacities: []riverstore.QueueCapacityTelemetry{
				{Queue: "heartbeat", Capacity: 1, Running: 1, Saturation: 0.5},
			},
		},
	}}
	// The ops kinds now ship at go_default, so the production operational
	// builder would demand a real postgres pool this test's fake database
	// cannot satisfy. This test only exercises the metrics path (registry-
	// bounded dimensions over queue telemetry), so the selected queue handlers are
	// demoted back to Celery here to keep it dormant and out of the way.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOperationalKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.newRiverClientID = func() string { return "test-client" }
	registry := health.NewRegistry(100 * time.Millisecond)
	_, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Service: "dev-health-worker", Queues: []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); err != nil {
		t.Fatalf("write worker metrics: %v", err)
	}
	for _, metric := range []string{
		`worker_jobs_available{queue="heartbeat",kind="system.heartbeat"} 3`,
		`worker_jobs_available{queue="retention",kind="system.retention_cleanup"} 2`,
		`worker_job_oldest_age_seconds{queue="heartbeat"} 12`,
		`worker_execution_saturation_ratio{queue="heartbeat"} 0.5`,
		`worker_domain_state_mismatch_total{domain_type="maintenance_run"} 0`,
	} {
		if !bytes.Contains(metrics.Bytes(), []byte(metric)) {
			t.Fatalf("worker metrics missing %q:\n%s", metric, metrics.String())
		}
	}
}

// celeryRoutedOperationalKinds is every operational queue kind. The checked-in
// contract now ships all five at go_default, so a genuinely dormant queue set has to be
// built explicitly by demoting them back to Celery in a scoped fixture.
var celeryRoutedOperationalKinds = []string{
	jobcontract.KindBillingNotification,
	jobcontract.KindWebhookDelivery,
	jobcontract.KindHeartbeat,
	jobcontract.KindRetentionCleanup,
	jobcontract.KindSyncCoverageRefresh,
}

func TestCeleryRoutedHandlersCannotPassQueueCompleteness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{domainSaturation: 0.25, queueSaturation: 0.5}
	_, contractRoot := demotedContractRoot(t, celeryRoutedOperationalKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	if len(components) != 3 || components[0].Name() != "postgres-runtime-pools" ||
		components[1].Name() != "queue-health-monitor" || components[2].Name() != "preclaim-readiness" {
		t.Fatalf("components = %#v, want pools, telemetry, and preclaim readiness", components)
	}
	if err := components[0].Start(context.Background()); err != nil {
		t.Fatalf("start pool lifecycle: %v", err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"queue_completeness"}) {
		t.Fatalf("readiness = %#v, want only queue_completeness failure", status)
	}
	if database.telemetryConfig.ClientID == "" || !slices.Equal(
		[]string{
			database.telemetryConfig.Queues[0].Name,
			database.telemetryConfig.Queues[1].Name,
			database.telemetryConfig.Queues[2].Name,
			database.telemetryConfig.Queues[3].Name,
		},
		[]string{"coverage", "heartbeat", "retention", "webhooks"},
	) || database.telemetryConfig.Queues[0].MaxWorkers != 1 ||
		database.telemetryConfig.Queues[1].MaxWorkers != 1 ||
		database.telemetryConfig.Queues[2].MaxWorkers != 1 ||
		database.telemetryConfig.Queues[3].MaxWorkers != 4 {
		t.Fatalf("queue telemetry did not use deployment capacities: %#v", database.telemetryConfig)
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); err != nil {
		t.Fatalf("write worker metrics: %v", err)
	}
	for _, metric := range []string{
		`worker_database_pool_saturation_ratio{pool="domain"} 0.25`,
		`worker_database_pool_saturation_ratio{pool="queue_control"} 0.5`,
	} {
		if !bytes.Contains(metrics.Bytes(), []byte(metric)) {
			t.Fatalf("worker metrics missing %q", metric)
		}
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown pool lifecycle: %v", err)
	}
	if !database.closed.Load() {
		t.Fatal("pool lifecycle did not close both runtime pools")
	}
}

// TestNoCompiledCapabilityClaimSurvives pins the CUT-02 removal of the
// compiled-kind advertisement. Capability is what a builder constructed; there
// is no list a dormant package can appear on to look registered.
func TestNoCompiledCapabilityClaimSurvives(t *testing.T) {
	t.Parallel()
	sources := productionWorkerDependencySources
	if sources.buildOperational == nil || sources.buildDaily == nil ||
		sources.buildWorkgraph == nil || sources.buildProviderSync == nil {
		t.Fatal("production dependency sources lost a concrete builder")
	}
}

func TestSelectedQueuesComposeMultipleBuilderFamilies(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, true)
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
		return runtimeRegistry, nil
	}
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
	reportKinds := map[string]bool{
		jobcontract.KindReportExecuteOnDemand:  true,
		jobcontract.KindReportExecuteScheduled: true,
	}
	// dailyKinds also covers the remaining-metrics families: their route is
	// cross-validated against families.json's unconditional "river" value,
	// so they stay executable regardless of this fixture and the fake
	// "daily" builder below must report them or queue completeness sees
	// uncovered registry kinds.
	dailyKinds := map[string]bool{
		jobcontract.KindDailyMetricsDispatch:     true,
		jobcontract.KindDailyMetricsPartition:    true,
		jobcontract.KindDailyMetricsFinalize:     true,
		jobcontract.KindRemainingCapacity:        true,
		jobcontract.KindRemainingComplexity:      true,
		jobcontract.KindRemainingDORA:            true,
		jobcontract.KindRemainingMembership:      true,
		jobcontract.KindRemainingRecommendations: true,
		jobcontract.KindRemainingReleaseImpact:   true,
	}
	// The real report builder is replaced by a fake here so the composition
	// rules are tested without a ClickHouse dependency; the production builder
	// has its own coverage.
	sources.buildReports = nil
	sources.buildOperational = fakeHandlerBuilder(
		"reports", selectSpecs(mustSelectedQueueSpecs(
			t, runtimeRegistry, "investment", "metrics", "reports", "workgraph",
		), reportKinds),
		jobruntime.QueueBudget{Queue: "reports", MaxWorkers: 2},
	)
	sources.buildDaily = fakeHandlerBuilder(
		"daily", selectSpecs(mustSelectedQueueSpecs(
			t, runtimeRegistry, "investment", "metrics", "reports", "workgraph",
		), dailyKinds),
		jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
	)

	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 []string{"metrics", "reports"},
			WorkerQueueConcurrency: map[string]int{"metrics": 2, "reports": 2},
			WorkerGroup:            "metrics-reports-test",
			ShutdownTimeout:        7_260 * time.Second,
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		health.NewRegistry(time.Second),
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 4 || components[2].Name() != "preclaim-readiness" ||
		components[3].Name() != "river-workers" {
		t.Fatalf("composed components = %#v", components)
	}
	processWorkers, ok := components[3].(workerProcessComponent)
	if !ok || len(processWorkers.components) != 1 ||
		processWorkers.components[0].Name() != "river-worker" ||
		processWorkers.ShutdownBudget() != 7_200*time.Second {
		t.Fatalf("worker process = %#v", components[3])
	}
}

func TestSelectedQueuesRejectDuplicateOrMissingBuilderHandlers(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for _, test := range []struct {
		name   string
		first  []string
		second []string
	}{
		{
			name: "duplicate",
			first: []string{
				jobcontract.KindReportExecuteOnDemand,
				jobcontract.KindReportExecuteScheduled,
			},
			second: []string{
				jobcontract.KindReportExecuteScheduled,
				jobcontract.KindDailyMetricsDispatch,
				jobcontract.KindDailyMetricsPartition,
				jobcontract.KindDailyMetricsFinalize,
			},
		},
		{
			name: "missing",
			first: []string{
				jobcontract.KindReportExecuteOnDemand,
			},
			second: []string{
				jobcontract.KindDailyMetricsDispatch,
				jobcontract.KindDailyMetricsPartition,
				jobcontract.KindDailyMetricsFinalize,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeRegistry, contractRoot := executableHeavyRegistry(t, true)
			database := &fakeWorkerDatabase{}
			sources := productionWorkerDependencySources
			sources.contractRoot = contractRoot
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return database, nil
			}
			sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
				return runtimeRegistry, nil
			}
			sources.buildReports = nil
			sources.buildOperational = fakeHandlerBuilder(
				"first", selectNamedSpecs(runtimeRegistry, test.first),
				jobruntime.QueueBudget{Queue: "reports", MaxWorkers: 2},
			)
			sources.buildDaily = fakeHandlerBuilder(
				"second", selectNamedSpecs(runtimeRegistry, test.second),
				jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
			)
			_, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{Queues: []string{"investment", "metrics", "reports", "workgraph"}, RiverDatabaseSchema: "river"},
				health.NewRegistry(time.Second),
				sources,
			)
			if !errors.Is(err, errWorkerDependencyUnavailable) || !database.closed.Load() {
				t.Fatalf("configure error=%v database_closed=%t", err, database.closed.Load())
			}
		})
	}
}

func TestProductionDailyBuilderFailsClosedWithoutClickHouse(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, false)
	ctx := context.Background()
	domainPool, err := pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, "postgresql://queue@127.0.0.1:1/devhealth")
	if err != nil {
		domainPool.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{
		pools: &postgres.RuntimePools{Domain: domainPool, QueueControl: queuePool},
	}
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
		return runtimeRegistry, nil
	}
	_, err = configureWorkerDependenciesWithSources(
		ctx,
		config.Config{
			Queues:                   []string{"investment", "metrics", "reports", "workgraph"},
			RiverDatabaseSchema:      "river",
			DomainDatabaseMaxConns:   4,
			QueueDatabaseMaxConns:    2,
			OperationalBridgeURL:     "http://localhost",
			OperationalBridgeToken:   secrets.NewValue("test-bridge-token"),
			OperationalBridgeTimeout: time.Second,
		},
		health.NewRegistry(time.Second),
		sources,
		slog.Default(),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("configure without ClickHouse = %v, want dependency refusal", err)
	}
}

func TestProductionOperationalBuilderConstructsNativeSyncCoverageRefresh(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, err := jobruntime.Load("contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	domainPool, err := pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, "postgresql://queue@127.0.0.1:1/devhealth")
	if err != nil {
		domainPool.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{
		pools: &postgres.RuntimePools{Domain: domainPool, QueueControl: queuePool},
	}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
		return runtimeRegistry, nil
	}
	components, err := configureWorkerDependenciesWithSources(
		ctx,
		config.Config{
			Queues:                   []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency:   map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:      "river",
			DomainDatabaseMaxConns:   4,
			QueueDatabaseMaxConns:    2,
			OperationalBridgeURL:     "http://localhost",
			OperationalBridgeToken:   secrets.NewValue("test-bridge-token"),
			OperationalBridgeTimeout: time.Second,
		},
		health.NewRegistry(time.Second),
		sources,
		slog.Default(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 4 || components[0].Name() != "postgres-runtime-pools" ||
		components[2].Name() != "preclaim-readiness" ||
		components[3].Name() != "river-workers" {
		t.Fatalf("production components = %#v", components)
	}
	queueWorkers, ok := components[3].(workerProcessComponent)
	if !ok || queueWorkers.presence == nil {
		t.Fatalf("production queue lifecycle = %#v", components[3])
	}
	presence, ok := queueWorkers.presence.(*jobruntime.WorkerPresence)
	if !ok || presence == nil {
		t.Fatalf("production worker presence = %#v", queueWorkers.presence)
	}
	if err := components[0].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerQueueSelectionRejectsProcessShutdownTimeoutBelowContract(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return &fakeWorkerDatabase{}, nil
	}
	dependencies := buildWorkerDependencies(context.Background(), config.Config{
		Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
		WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
		ShutdownTimeout:        959 * time.Second,
	}, sources)
	defer dependencies.close()
	if !errors.Is(dependencies.startupErr, errWorkerDependencyUnavailable) {
		t.Fatalf("startup error = %v, want shutdown contract refusal", dependencies.startupErr)
	}
}

func TestPreclaimReadinessRefusesFailedDependenciesBeforeConsumersStart(t *testing.T) {
	t.Parallel()
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("database", func(context.Context) error {
		return errors.New("database unavailable")
	}); err != nil {
		t.Fatal(err)
	}
	component := preclaimReadinessComponent{registry: registry}
	if err := component.Start(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("Start() error = %v, want preclaim dependency refusal", err)
	}
}

// A preclaim refusal aborts Start, so the process exits before its operator
// HTTP surface can be scraped: the log line is the ONLY place the failing
// check names are ever observable. It must name exactly the checks that
// refused, and must not leak the dependency error strings behind them.
func TestPreclaimReadinessNamesTheChecksThatRefused(t *testing.T) {
	t.Parallel()
	const secret = "postgresql://devhealth_domain:hunter2@postgres:5432/devhealth"
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error {
		return errors.New("dial " + secret)
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("river_schema", func(context.Context) error {
		return errors.New("schema missing")
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("job_registry", func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	component := preclaimReadinessComponent{
		registry: registry,
		logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	if err := component.Start(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("Start() error = %v, want preclaim dependency refusal", err)
	}

	var record struct {
		Message       string `json:"msg"`
		ErrorCategory string `json:"error_category"`
		FailedChecks  string `json:"failed_checks"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &record); err != nil {
		t.Fatalf("decode log record: %v (raw %q)", err, logs.String())
	}
	if record.ErrorCategory != "dependency_unavailable" {
		t.Errorf("error_category = %q, want dependency_unavailable", record.ErrorCategory)
	}
	// Registry.CheckRequired sorts, so this is an exact-set assertion, not a
	// containment one: a check that passed must not appear.
	if record.FailedChecks != "domain_postgres,river_schema" {
		t.Errorf("failed_checks = %q, want \"domain_postgres,river_schema\"", record.FailedChecks)
	}
	if strings.Contains(logs.String(), secret) || strings.Contains(logs.String(), "hunter2") {
		t.Errorf("preclaim log leaked dependency error detail: %s", logs.String())
	}
}

// The mirror image: a satisfied preclaim starts silently. Without this, a
// refusal-naming regression that logged unconditionally would still pass the
// test above.
func TestPreclaimReadinessLogsNothingWhenDependenciesPass(t *testing.T) {
	t.Parallel()
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	component := preclaimReadinessComponent{
		registry: registry,
		logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if logs.Len() != 0 {
		t.Errorf("preclaim logged on a ready registry: %s", logs.String())
	}
}

func TestUnsupportedAvailableContractVersionFailsClosed(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshot: riverstore.QueueTelemetrySnapshot{
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "heartbeat", Kind: "system.heartbeat"},
				{Queue: "retention", Kind: "system.retention_cleanup"},
			},
			Queues: []riverstore.QueueAgeTelemetry{{Queue: "heartbeat"}, {Queue: "retention"}},
		},
		checkErr: riverstore.ErrUnsupportedAvailableContractVersion,
	}}
	// Demote the ops kinds so the production operational builder (which now
	// requires a real postgres pool once they are executable) stays a
	// no-op; this test only exercises the queued-contract-versions gate.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOperationalKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	sources.newRiverClientID = func() string { return "test-client" }

	registry := health.NewRegistry(100 * time.Millisecond)
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
		},
		registry,
		sources,
	); err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	want := []string{"queue_completeness", "queued_contract_versions"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestQueueTelemetryFailureMakesMetricsUnavailable(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshotErr: errors.New("postgresql://queue:secret@db/app"),
	}}
	// Demote the ops kinds so the production operational builder (which now
	// requires a real postgres pool once they are executable) stays a
	// no-op; this test only exercises the queue-telemetry metrics path.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOperationalKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	sources.newRiverClientID = func() string { return "test-client" }

	registry := health.NewRegistry(100 * time.Millisecond)
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
		},
		registry,
		sources,
	); err != nil {
		t.Fatal(err)
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("WriteMetrics() error = %v, want stable unavailable error", err)
	}
	if metrics.Len() != 0 {
		t.Fatalf("failed scrape emitted partial metrics:\n%s", metrics.String())
	}
}

func TestMissingContractArtifactsFailRegistryAndQueueChecks(t *testing.T) {
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.contractRoot = filepath.Join(t.TempDir(), "missing-contracts")

	registry := health.NewRegistry(100 * time.Millisecond)
	_, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	status := registry.Readiness(context.Background())
	want := []string{"job_registry", "queue_completeness"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReadinessRegistrationFailureClosesConstructedPools(t *testing.T) {
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.contractRoot = filepath.Join(t.TempDir(), "missing-contracts")

	registry := health.NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("register collision: %v", err)
	}
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, RiverDatabaseSchema: "river"},
		registry,
		sources,
	); err == nil {
		t.Fatal("duplicate readiness registration unexpectedly succeeded")
	}
	if !database.closed.Load() {
		t.Fatal("registration failure leaked constructed runtime pools")
	}
}

func TestPoolReadinessErrorsAreCollapsedToStableFailure(t *testing.T) {
	database := &fakeWorkerDatabase{
		domainErr: errors.New("postgresql://domain:secret@db/app"),
		queueErr:  errors.New("postgresql://queue:secret@db/app"),
		schemaErr: errors.New("raw driver detail"),
	}
	dependencies := &workerDependencies{database: database}
	if err := dependencies.domainReady(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("domainReady() error = %v", err)
	}
	if err := dependencies.queueReady(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("queueReady() error = %v", err)
	}
	if err := dependencies.riverSchemaReady("river")(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("riverSchemaReady() error = %v", err)
	}
}

type fakeWorkerDatabase struct {
	domainErr            error
	queueErr             error
	schemaErr            error
	domainSaturation     float64
	queueSaturation      float64
	telemetry            queueTelemetrySampler
	telemetryErr         error
	telemetryConfig      riverstore.QueueTelemetryConfig
	closed               atomic.Bool
	acquireObserver      postgres.PoolAcquireObserver
	projectsV2Configured bool
	projectsV2Err        error
	projectsV2Queries    int
}

type namedComponent string

func (component namedComponent) Name() string         { return string(component) }
func (namedComponent) Start(context.Context) error    { return nil }
func (namedComponent) Shutdown(context.Context) error { return nil }

type blockingShutdownComponent struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

type recordingWorkerPresence struct {
	started  chan struct{}
	draining chan struct{}
	removed  chan struct{}
}

func newRecordingWorkerPresence() *recordingWorkerPresence {
	return &recordingWorkerPresence{
		started: make(chan struct{}), draining: make(chan struct{}), removed: make(chan struct{}),
	}
}

func (presence *recordingWorkerPresence) Start(context.Context) error {
	close(presence.started)
	return nil
}

func (presence *recordingWorkerPresence) BeginDrain(context.Context) error {
	close(presence.draining)
	return nil
}

func (presence *recordingWorkerPresence) Shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	close(presence.removed)
	return nil
}

func (*recordingWorkerPresence) Errors() <-chan error { return nil }

type failingStartComponent struct{ err error }

func (failingStartComponent) Name() string                          { return "failing" }
func (component failingStartComponent) Start(context.Context) error { return component.err }
func (failingStartComponent) Shutdown(context.Context) error        { return nil }

type deadlineShutdownComponent struct{ deadline chan time.Time }

func (*deadlineShutdownComponent) Name() string                { return "started" }
func (*deadlineShutdownComponent) Start(context.Context) error { return nil }
func (component *deadlineShutdownComponent) Shutdown(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("rollback context has no deadline")
	}
	component.deadline <- deadline
	return nil
}

func TestWorkerProcessStartRollbackUsesConfiguredShutdownBudget(t *testing.T) {
	t.Parallel()
	started := &deadlineShutdownComponent{deadline: make(chan time.Time, 1)}
	budget := time.Second
	component := workerProcessComponent{
		components: []lifecycle.Component{
			started,
			failingStartComponent{err: errors.New("start failed")},
		},
		budget: budget,
	}
	before := time.Now()
	if err := component.Start(context.Background()); err == nil {
		t.Fatal("Start() succeeded after a child start failure")
	}
	deadline := <-started.deadline
	if deadline.Before(before) || deadline.After(before.Add(budget+100*time.Millisecond)) {
		t.Fatalf("rollback deadline = %v, want within reviewed budget", deadline)
	}
}

func (component *blockingShutdownComponent) Name() string      { return component.name }
func (*blockingShutdownComponent) Start(context.Context) error { return nil }
func (component *blockingShutdownComponent) Shutdown(context.Context) error {
	close(component.entered)
	<-component.release
	return nil
}

func TestWorkerProcessComponentStopsChildrenConcurrently(t *testing.T) {
	t.Parallel()
	first := &blockingShutdownComponent{
		name: "first", entered: make(chan struct{}), release: make(chan struct{}),
	}
	second := &blockingShutdownComponent{
		name: "second", entered: make(chan struct{}), release: make(chan struct{}),
	}
	component := workerProcessComponent{
		components: []lifecycle.Component{first, second}, budget: time.Minute,
	}
	done := make(chan error, 1)
	go func() { done <- component.Shutdown(context.Background()) }()
	for _, entered := range []<-chan struct{}{first.entered, second.entered} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("worker children did not enter shutdown concurrently")
		}
	}
	close(first.release)
	close(second.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPresenceWaitsForChildrenBeforeRemoval(t *testing.T) {
	t.Parallel()
	child := &blockingShutdownComponent{
		name: "child", entered: make(chan struct{}), release: make(chan struct{}),
	}
	presence := newRecordingWorkerPresence()
	component := workerProcessComponent{
		components: []lifecycle.Component{child}, budget: time.Minute, presence: presence,
	}
	done := make(chan error, 1)
	go func() { done <- component.Shutdown(context.Background()) }()

	select {
	case <-presence.draining:
	case <-time.After(time.Second):
		t.Fatal("worker presence was not marked draining")
	}
	select {
	case <-child.entered:
	case <-time.After(time.Second):
		t.Fatal("worker child did not enter shutdown")
	}
	select {
	case <-presence.removed:
		t.Fatal("worker presence was removed while a worker child was running")
	default:
	}
	close(child.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-presence.removed:
	case <-time.After(time.Second):
		t.Fatal("worker presence was not removed after the worker child stopped")
	}
}

func TestWorkerPresenceSurvivesExpiredShutdownAttempt(t *testing.T) {
	t.Parallel()
	child := &blockingShutdownComponent{
		name: "child", entered: make(chan struct{}), release: make(chan struct{}),
	}
	presence := newRecordingWorkerPresence()
	component := workerProcessComponent{
		components: []lifecycle.Component{child}, budget: time.Minute, presence: presence,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- component.Shutdown(ctx) }()

	select {
	case <-presence.draining:
	case <-time.After(time.Second):
		t.Fatal("worker presence was not marked draining")
	}
	select {
	case <-child.entered:
	case <-time.After(time.Second):
		t.Fatal("worker child did not enter shutdown")
	}
	<-ctx.Done()
	select {
	case <-done:
		t.Fatal("worker shutdown returned while a worker child was running")
	default:
	}
	select {
	case <-presence.removed:
		t.Fatal("worker presence was removed after the shutdown attempt expired")
	default:
	}

	close(child.release)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	select {
	case <-presence.removed:
		t.Fatal("expired shutdown attempt deleted worker presence")
	default:
	}
}

func fakeHandlerBuilder(
	_ string,
	specs []jobruntime.HandlerSpec,
	queues ...jobruntime.QueueBudget,
) workerFamilyBuilder {
	return func(
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
		*river.Workers,
	) (workerFamily, error) {
		return workerFamily{
			handlers: specs, queues: queues,
		}, nil
	}
}

func fakeRiverProcessBuilder(name string) workerProcessBuilder {
	return func(
		config.Config,
		workerDatabase,
		*river.Workers,
		workerFamily,
		*slog.Logger,
	) (lifecycle.Component, error) {
		return namedComponent(name), nil
	}
}

func executableHeavyRegistry(
	t *testing.T,
	promoteReports bool,
) (*jobruntime.Registry, string) {
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
	// The checked-in contract now ships every batch queue kind at
	// go_default, but this fixture only exercises the daily-metrics and
	// (optionally) report builders. work-graph and investment have no
	// builder in this binary at all, so they are demoted back to Celery here
	// -- otherwise the selected queue set would demand handlers nothing ever
	// constructs. The remaining-metrics families are left alone: their route
	// is cross-validated against families.json's own (unconditional) "river"
	// value, so demoting them independently would make the fixture
	// internally inconsistent rather than scoped.
	demoted := map[string]bool{
		jobcontract.KindWorkGraphBuild:        true,
		jobcontract.KindInvestmentMaterialize: true,
		jobcontract.KindInvestmentDispatch:    true,
		jobcontract.KindInvestmentChunk:       true,
		jobcontract.KindInvestmentFinalize:    true,
	}
	for _, job := range document.Jobs {
		kind, _ := job["kind"].(string)
		switch {
		case strings.HasPrefix(kind, "metrics.daily_"):
			job["state"] = "go_default"
			job["route"] = "river"
		case strings.HasPrefix(kind, "report.execute_"):
			if promoteReports {
				job["state"] = "go_default"
				job["route"] = "river"
			} else {
				job["state"] = "go_implemented"
				job["route"] = "celery"
				job["rollback_route"] = "celery"
			}
		case demoted[kind]:
			job["state"] = "go_implemented"
			job["route"] = "celery"
			job["rollback_route"] = "celery"
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

func selectSpecs(
	specs []jobruntime.HandlerSpec,
	kinds map[string]bool,
) []jobruntime.HandlerSpec {
	var result []jobruntime.HandlerSpec
	for _, spec := range specs {
		if kinds[spec.Kind] {
			result = append(result, spec)
		}
	}
	return result
}

func mustSelectedQueueSpecs(
	t *testing.T,
	registry *jobruntime.Registry,
	queues ...string,
) []jobruntime.HandlerSpec {
	t.Helper()
	specs, err := registry.SelectedQueues(queues)
	if err != nil {
		t.Fatal(err)
	}
	return specs
}

func selectNamedSpecs(
	registry *jobruntime.Registry,
	kinds []string,
) []jobruntime.HandlerSpec {
	result := make([]jobruntime.HandlerSpec, 0, len(kinds))
	for _, kind := range kinds {
		spec, _ := registry.Descriptor(kind)
		result = append(result, spec)
	}
	return result
}

func (database *fakeWorkerDatabase) DomainReady(context.Context) error {
	return database.domainErr
}

func (database *fakeWorkerDatabase) GitHubProjectsV2Configured(context.Context) (bool, error) {
	database.projectsV2Queries++
	return database.projectsV2Configured, database.projectsV2Err
}

func (database *fakeWorkerDatabase) QueueReady(context.Context) error {
	return database.queueErr
}

func (database *fakeWorkerDatabase) RiverSchemaReady(context.Context, string) error {
	return database.schemaErr
}

func (database *fakeWorkerDatabase) PoolSaturation() (float64, float64) {
	return database.domainSaturation, database.queueSaturation
}

func (database *fakeWorkerDatabase) NewQueueTelemetrySampler(
	config riverstore.QueueTelemetryConfig,
) (queueTelemetrySampler, error) {
	database.telemetryConfig = config
	if database.telemetryErr != nil {
		return nil, database.telemetryErr
	}
	if database.telemetry != nil {
		return database.telemetry, nil
	}
	snapshot := riverstore.QueueTelemetrySnapshot{}
	for _, job := range config.Jobs {
		snapshot.Jobs = append(snapshot.Jobs, riverstore.QueueJobTelemetry{Queue: job.Queue, Kind: job.Kind})
	}
	for _, queue := range config.Queues {
		snapshot.Queues = append(snapshot.Queues, riverstore.QueueAgeTelemetry{Queue: queue.Name})
	}
	return &fakeQueueTelemetry{snapshot: snapshot}, nil
}

func (database *fakeWorkerDatabase) AttachPoolAcquireObserver(observer postgres.PoolAcquireObserver) {
	database.acquireObserver = observer
}

func (database *fakeWorkerDatabase) Close() {
	database.closed.Store(true)
}

type fakeQueueTelemetry struct {
	snapshot    riverstore.QueueTelemetrySnapshot
	snapshotErr error
	checkErr    error
}

func (telemetry *fakeQueueTelemetry) Snapshot(context.Context) (riverstore.QueueTelemetrySnapshot, error) {
	return telemetry.snapshot, telemetry.snapshotErr
}

func (telemetry *fakeQueueTelemetry) CheckAvailableContractVersions(context.Context) error {
	return telemetry.checkErr
}

// TestExecutableReportKindsWithoutAdaptersCloseReadiness is the CUT-02
// acceptance proof. With both report kinds routed to River, a heavy worker that
// constructs only the daily adapters must refuse to start: the registry says
// two more kinds are fetchable and nothing in this binary can execute them.
func TestExecutableReportKindsWithoutAdaptersCloseReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, true)
	ctx := context.Background()
	domainPool, err := pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer domainPool.Close()
	queuePool, err := pgxpool.New(ctx, "postgresql://queue@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer queuePool.Close()
	database := &postgresWorkerDatabase{
		pools: &postgres.RuntimePools{Domain: domainPool, QueueControl: queuePool},
	}
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
		return runtimeRegistry, nil
	}
	// The report family is deliberately absent, exactly as production was
	// before CUT-03 wired it.
	sources.buildReports = nil

	_, err = configureWorkerDependenciesWithSources(
		ctx,
		config.Config{
			Queues:                   []string{"investment", "metrics", "reports", "workgraph"},
			RiverDatabaseSchema:      "river",
			DomainDatabaseMaxConns:   4,
			QueueDatabaseMaxConns:    2,
			OperationalBridgeURL:     "http://localhost",
			OperationalBridgeToken:   secrets.NewValue("test-bridge-token"),
			OperationalBridgeTimeout: time.Second,
		},
		health.NewRegistry(time.Second),
		sources,
		slog.Default(),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("configure error = %v, want unconstructed report kinds to close readiness", err)
	}
}

// TestFakeBuilderCannotSatisfyQueueCoverage proves a builder cannot buy
// readiness by inventing handler specs. Coverage counts alone are not enough:
// every constructed spec is compared field by field with the registry.
func TestFakeBuilderCannotSatisfyQueueCoverage(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, true)
	heavy := mustSelectedQueueSpecs(t, runtimeRegistry, "investment", "metrics", "reports", "workgraph")
	reports := selectSpecs(heavy, map[string]bool{
		jobcontract.KindReportExecuteOnDemand:  true,
		jobcontract.KindReportExecuteScheduled: true,
	})
	daily := selectSpecs(heavy, map[string]bool{
		jobcontract.KindDailyMetricsDispatch:  true,
		jobcontract.KindDailyMetricsPartition: true,
		jobcontract.KindDailyMetricsFinalize:  true,
	})

	for _, test := range []struct {
		name   string
		mutate func([]jobruntime.HandlerSpec)
	}{
		{"drifted timeout", func(specs []jobruntime.HandlerSpec) {
			specs[0].Timeout += time.Second
		}},
		{"drifted route", func(specs []jobruntime.HandlerSpec) {
			specs[0].Route = "celery"
		}},
		{"drifted queue", func(specs []jobruntime.HandlerSpec) {
			specs[0].Queue = "metrics"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			forged := append([]jobruntime.HandlerSpec(nil), reports...)
			test.mutate(forged)
			database := &fakeWorkerDatabase{}
			sources := productionWorkerDependencySources
			sources.contractRoot = contractRoot
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return database, nil
			}
			sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
				return runtimeRegistry, nil
			}
			sources.buildReports = nil
			sources.buildOperational = fakeHandlerBuilder(
				"forged-reports", forged,
				jobruntime.QueueBudget{Queue: "reports", MaxWorkers: 2},
			)
			sources.buildDaily = fakeHandlerBuilder(
				"daily", daily,
				jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
			)
			sources.buildWorkgraph = nil
			_, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{
					Queues:                 []string{"metrics"},
					RiverDatabaseSchema:    "river",
					DomainDatabaseMaxConns: 4,
					QueueDatabaseMaxConns:  2,
				},
				health.NewRegistry(time.Second),
				sources,
			)
			if !errors.Is(err, errWorkerDependencyUnavailable) {
				t.Fatalf("configure error = %v, want forged handler rejection", err)
			}
		})
	}
}

// TestConstructedQueueBudgetMustMatchDeploymentManifest proves the runtime
// cannot quietly consume a queue at a capacity the reviewed manifest never
// budgeted.
func TestConstructedQueueBudgetMustMatchDeploymentManifest(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, false)
	// The remaining-metrics families stay executable regardless of this
	// fixture (their route is cross-validated against families.json's
	// unconditional "river" value), so the fake "daily" builder below must
	// report them too or queue completeness sees uncovered registry kinds
	// before the queue-budget assertions this test actually exercises ever
	// run.
	daily := selectSpecs(mustSelectedQueueSpecs(
		t, runtimeRegistry, "investment", "metrics", "reports", "workgraph",
	), map[string]bool{
		jobcontract.KindDailyMetricsDispatch:     true,
		jobcontract.KindDailyMetricsPartition:    true,
		jobcontract.KindDailyMetricsFinalize:     true,
		jobcontract.KindRemainingCapacity:        true,
		jobcontract.KindRemainingComplexity:      true,
		jobcontract.KindRemainingDORA:            true,
		jobcontract.KindRemainingMembership:      true,
		jobcontract.KindRemainingRecommendations: true,
		jobcontract.KindRemainingReleaseImpact:   true,
	})
	for _, test := range []struct {
		name    string
		queue   jobruntime.QueueBudget
		wantErr bool
	}{
		{
			name:  "matching budget",
			queue: jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
		},
		{
			name:    "over-budget workers",
			queue:   jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 8},
			wantErr: true,
		},
		{
			name:    "unbudgeted queue",
			queue:   jobruntime.QueueBudget{Queue: "metrics_shadow", MaxWorkers: 2},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := &fakeWorkerDatabase{}
			sources := productionWorkerDependencySources
			sources.contractRoot = contractRoot
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return database, nil
			}
			sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
				return runtimeRegistry, nil
			}
			sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
			sources.buildReports = nil
			sources.buildWorkgraph = nil
			sources.buildOperational = nil
			sources.buildDaily = fakeHandlerBuilder("daily", daily, test.queue)
			_, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{
					Queues:                 []string{"metrics"},
					WorkerQueueConcurrency: map[string]int{"metrics": 2},
					ShutdownTimeout:        7_260 * time.Second,
					RiverDatabaseSchema:    "river",
					DomainDatabaseMaxConns: 4,
					QueueDatabaseMaxConns:  2,
				},
				health.NewRegistry(time.Second),
				sources,
			)
			if test.wantErr != errors.Is(err, errWorkerDependencyUnavailable) {
				t.Fatalf("configure error = %v, wantErr = %t", err, test.wantErr)
			}
		})
	}
}

// TestSelectedQueueCapabilityIsValidatedAcrossBuilderFamilies proves a handler
// contributed by a separate builder is covered by exact startup validation.
// All builders now register into one River client, and capability reaches
// validation through one canonical channel before that client starts.
func TestSelectedQueueCapabilityIsValidatedAcrossBuilderFamilies(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := promotedContractRoot(t, jobcontract.KindTeamAutoimport)
	providerSpec, ok := runtimeRegistry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok {
		t.Fatal("sync.provider_unit descriptor missing")
	}
	autoimportSpec, ok := runtimeRegistry.Descriptor(jobcontract.KindTeamAutoimport)
	if !ok {
		t.Fatal("sync.team_autoimport descriptor missing")
	}

	syncConfig := config.Config{
		Queues:                 []string{"sync", "sync_provider"},
		WorkerQueueConcurrency: map[string]int{"sync": 4, "sync_provider": 2},
		ShutdownTimeout:        7_260 * time.Second,
		RiverDatabaseSchema:    "river",
		DomainDatabaseMaxConns: 4,
		QueueDatabaseMaxConns:  2,
	}
	baseSources := func() workerDependencySources {
		sources := productionWorkerDependencySources
		sources.contractRoot = contractRoot
		sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
			return runtimeRegistry, nil
		}
		sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
		sources.buildProviderSync = func(
			context.Context, config.Config, workerDatabase,
			*jobruntime.Registry, jobruntime.Observer, *slog.Logger, *river.Workers,
		) (workerFamily, error) {
			return workerFamily{
				handlers: []jobruntime.HandlerSpec{providerSpec},
				queues: []jobruntime.QueueBudget{
					{Queue: "sync_provider", MaxWorkers: 2},
				},
			}, nil
		}
		return sources
	}

	for _, test := range []struct {
		name      string
		reported  bool
		wantReady bool
	}{
		{name: "unreported nested handler closes readiness"},
		{name: "reported nested handler completes the queue set", reported: true, wantReady: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := &fakeWorkerDatabase{}
			sources := baseSources()
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return database, nil
			}
			sources.buildSyncCoordinator = func(
				context.Context, config.Config, workerDatabase, *jobruntime.Registry,
				jobruntime.Observer, *slog.Logger, *river.Workers,
			) (workerFamily, error) {
				family := workerFamily{}
				if test.reported {
					family.handlers = []jobruntime.HandlerSpec{autoimportSpec}
					family.queues = []jobruntime.QueueBudget{
						{Queue: "sync", MaxWorkers: 4},
					}
				}
				return family, nil
			}
			registry := health.NewRegistry(100 * time.Millisecond)
			_, err := configureWorkerDependenciesWithSources(
				context.Background(), syncConfig, registry, sources,
			)
			if test.wantReady {
				if err != nil {
					t.Fatalf("configure error = %v", err)
				}
			} else if !errors.Is(err, errWorkerDependencyUnavailable) {
				t.Fatalf("configure error = %v, want unavailable", err)
			}
			if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			status := registry.Readiness(context.Background())
			if failed := slices.Contains(status.Failed, "queue_completeness"); failed == test.wantReady {
				t.Fatalf("readiness = %#v, want queue_completeness failure = %t",
					status, !test.wantReady)
			}
		})
	}
}

// TestOperationalDatabaseFailureCrashesInsteadOfIdlingUnready is CHAOS-3873
// evidence. A DSN that cannot be opened for an operational reason used to
// return nil, nil: the shell started, readiness failed forever, and nothing
// terminated the process. Declared configuration rejections keep their
// live-but-unready behaviour, because those surface as named readiness checks.
func TestOperationalDatabaseFailureCrashesInsteadOfIdlingUnready(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	cfg := config.Config{
		Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
		WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
		RiverDatabaseSchema:    "river",
	}

	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return nil, errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")
	}
	components, err := configureWorkerDependenciesWithSources(
		context.Background(), cfg, health.NewRegistry(100*time.Millisecond), sources,
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) || len(components) != 0 {
		t.Fatalf("operational open failure = %v, %d components; want dependency refusal", err, len(components))
	}
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "worker_database_open_failed" {
		t.Fatalf("reason code = %v, want worker_database_open_failed", err)
	}

	rejected := productionWorkerDependencySources
	rejected.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return nil, postgres.ErrQueueControlTransactionMode
	}
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(), cfg, health.NewRegistry(100*time.Millisecond), rejected,
	); err != nil {
		t.Fatalf("configuration rejection must stay live and unready, got %v", err)
	}
}

// TestUnsetShutdownTimeoutIsDerivedFromTheSelectedQueues is CHAOS-3873
// evidence: the 30s package default yields a NEGATIVE drain budget, so every
// default-configured worker failed with the opaque sentinel. An unset timeout
// is derived from the selection; a value the operator chose still fails closed.
func TestUnsetShutdownTimeoutIsDerivedFromTheSelectedQueues(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return &fakeWorkerDatabase{}, nil
	}
	base := config.Config{
		Queues:                 []string{"coverage", "heartbeat", "retention", "webhooks"},
		WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
	}

	defaulted := base
	defaulted.ShutdownTimeout = config.DefaultShutdownTimeout
	derived := buildWorkerDependencies(context.Background(), defaulted, sources)
	defer derived.close()
	if derived.startupErr != nil {
		t.Fatalf("default-configured worker refused to start: %v", derived.startupErr)
	}
	if derived.workerDrainBudget <= 0 {
		t.Fatalf("derived drain budget = %s, want a positive window", derived.workerDrainBudget)
	}

	chosen := base
	chosen.ShutdownTimeout = config.DefaultShutdownTimeout
	chosen.ShutdownTimeoutExplicit = true
	refused := buildWorkerDependencies(context.Background(), chosen, sources)
	defer refused.close()
	if !errors.Is(refused.startupErr, errWorkerDependencyUnavailable) {
		t.Fatalf("explicit 30s timeout = %v, want shutdown contract refusal", refused.startupErr)
	}
}

// TestUnsupportedContractRefusalNamesTheContractOncePerChange covers both
// halves of the diagnostic added for CHAOS-3938: the refusal must NAME the
// offending queue/kind/version, and it must say so once per change rather than
// once per readiness evaluation. Readiness is re-run on every /readyz probe
// and /metrics scrape and a failing check does not stop a running process, so
// logging per evaluation would turn one incompatible row into probe-rate ERROR
// on every replica.
func TestUnsupportedContractRefusalNamesTheContractOncePerChange(t *testing.T) {
	logs := &bytes.Buffer{}
	telemetry := &fakeQueueTelemetry{
		checkErr: &riverstore.UnsupportedContractVersionError{
			Offenders: []string{"sync/dispatch_sync_run@7"},
		},
	}
	dependencies := &workerDependencies{
		queueTelemetry:         telemetry,
		queueTelemetryRequired: true,
		logger:                 slog.New(slog.NewTextHandler(logs, nil)),
	}

	for range 3 {
		if err := dependencies.queuedContractVersionsReady(context.Background()); err == nil {
			t.Fatal("unsupported contract version was accepted")
		}
	}
	if lines := countLogLines(logs.String()); lines != 1 {
		t.Fatalf("logged %d times for one unchanged offender set, want 1: %s", lines, logs.String())
	}
	if !strings.Contains(logs.String(), "sync/dispatch_sync_run@7") {
		t.Fatalf("refusal did not name the offending contract: %s", logs.String())
	}

	// A different offender set is new information.
	telemetry.checkErr = &riverstore.UnsupportedContractVersionError{
		Offenders: []string{"sync/post_sync@9"},
	}
	if err := dependencies.queuedContractVersionsReady(context.Background()); err == nil {
		t.Fatal("unsupported contract version was accepted")
	}
	if lines := countLogLines(logs.String()); lines != 2 {
		t.Fatalf("a changed offender set logged %d times in total, want 2: %s", lines, logs.String())
	}

	// So is a recurrence after the queue drained clean.
	telemetry.checkErr = nil
	if err := dependencies.queuedContractVersionsReady(context.Background()); err != nil {
		t.Fatalf("clean queue refused readiness: %v", err)
	}
	if lines := countLogLines(logs.String()); lines != 2 {
		t.Fatalf("recovery logged: %s", logs.String())
	}
	telemetry.checkErr = &riverstore.UnsupportedContractVersionError{
		Offenders: []string{"sync/post_sync@9"},
	}
	if err := dependencies.queuedContractVersionsReady(context.Background()); err == nil {
		t.Fatal("unsupported contract version was accepted")
	}
	if lines := countLogLines(logs.String()); lines != 3 {
		t.Fatalf("a recurrence after recovery logged %d times in total, want 3: %s", lines, logs.String())
	}
}

func countLogLines(output string) int {
	lines := 0
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	return lines
}
