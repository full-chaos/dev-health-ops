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
)

func TestWorkerSpecConfiguresDependencies(t *testing.T) {
	if workerSpec.Service != "dev-health-worker" || workerSpec.DefaultProfile != "latency" {
		t.Fatalf("unexpected worker spec: %#v", workerSpec)
	}
	if !slices.Equal(workerSpec.Profiles, []string{"latency", "sync", "heavy", "ops"}) {
		t.Fatalf("unexpected worker profiles: %v", workerSpec.Profiles)
	}
	if workerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal("worker dependency configuration is not wired")
	}
}

func TestNoDatabaseConfigurationStaysLiveAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependencies(
		context.Background(),
		config.Config{Profile: "latency", RiverDatabaseSchema: "river"},
		registry,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no pool lifecycle without DSNs", len(components))
	}
	if registry.RequiredCount() != 7 {
		t.Fatalf("required checks = %d, want 7", registry.RequiredCount())
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); err != nil {
		t.Fatalf("write worker metrics: %v", err)
	}
	for _, metric := range []string{
		`,profile="latency"} 1`,
		`worker_execution_saturation_ratio{profile="latency"} 0`,
		`worker_database_pool_saturation_ratio{pool="domain"} 0`,
	} {
		if !bytes.Contains(metrics.Bytes(), []byte(metric)) {
			t.Fatalf("worker metrics missing %q:\n%s", metric, metrics.String())
		}
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	status := registry.Readiness(context.Background())
	want := []string{"domain_postgres", "profile_completeness", "queue_postgres", "river_schema"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestTransactionModeQueueControlHasActionableReadinessCategory(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependencies(
		context.Background(),
		config.Config{
			Service:                 "dev-health-worker",
			Profile:                 "ops",
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
		"profile_completeness",
		"queue_control_config",
		"queue_postgres",
		"queued_contract_versions",
		"river_schema",
	}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want sanitized failures %v", status, want)
	}
}

func TestOpsProfileMetricsUseRegistryBoundedJobDimensions(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshot: riverstore.QueueTelemetrySnapshot{
			Profile: "ops",
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "heartbeat", Kind: "system.heartbeat", Available: 3},
				{Queue: "retention", Kind: "system.retention_cleanup", Available: 2},
			},
			Queues: []riverstore.QueueAgeTelemetry{
				{Queue: "heartbeat", OldestAvailableAge: 12 * time.Second},
				{Queue: "retention", OldestAvailableAge: 4 * time.Second},
			},
			ExecutionSaturation: 0.5,
		},
	}}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.newRiverClientID = func() string { return "test-client" }
	registry := health.NewRegistry(100 * time.Millisecond)
	_, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Service: "dev-health-worker", Profile: "ops", RiverDatabaseSchema: "river"},
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
		`worker_jobs_available{profile="ops",queue="heartbeat",kind="system.heartbeat"} 3`,
		`worker_jobs_available{profile="ops",queue="retention",kind="system.retention_cleanup"} 2`,
		`worker_job_oldest_age_seconds{profile="ops",queue="heartbeat"} 12`,
		`worker_execution_saturation_ratio{profile="ops"} 0.5`,
		`worker_domain_state_mismatch_total{domain_type="maintenance_run"} 0`,
	} {
		if !bytes.Contains(metrics.Bytes(), []byte(metric)) {
			t.Fatalf("worker metrics missing %q:\n%s", metric, metrics.String())
		}
	}
}

func TestCeleryRoutedHandlersCannotPassProfileCompleteness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatalf("load runtime registry: %v", err)
	}
	database := &fakeWorkerDatabase{domainSaturation: 0.25, queueSaturation: 0.5}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.compiledHandlers = func(profile string) []jobruntime.HandlerSpec {
		return runtimeRegistry.Profile(profile)
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	if len(components) != 1 || components[0].Name() != "postgres-runtime-pools" {
		t.Fatalf("components = %#v, want PostgreSQL pool lifecycle", components)
	}
	if err := components[0].Start(context.Background()); err != nil {
		t.Fatalf("start pool lifecycle: %v", err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"profile_completeness"}) {
		t.Fatalf("readiness = %#v, want only profile_completeness failure", status)
	}
	if database.telemetryConfig.ClientID == "" || !slices.Equal(
		[]string{database.telemetryConfig.Queues[0].Name, database.telemetryConfig.Queues[1].Name},
		[]string{"heartbeat", "retention"},
	) || database.telemetryConfig.Queues[0].MaxWorkers != 1 || database.telemetryConfig.Queues[1].MaxWorkers != 1 {
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

func TestHeavyHandlersAdvertiseDormantCompiledCapability(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	handlers := compiledWorkerHandlers("heavy")
	if len(handlers) != len(compiledHeavyHandlerKinds) {
		t.Fatalf("heavy handlers = %d, want %d", len(handlers), len(compiledHeavyHandlerKinds))
	}
	kinds := make(map[string]struct{}, len(handlers))
	for _, handler := range handlers {
		if handler.Profile != "heavy" || handler.MigrationState != "go_implemented" ||
			handler.Route != "celery" || handler.RollbackRoute != "celery" ||
			handler.Executable() {
			t.Fatalf("handler unexpectedly active: %#v", handler)
		}
		kinds[handler.Kind] = struct{}{}
	}
	if len(kinds) != len(handlers) {
		t.Fatalf("heavy kinds are not independently compiled: %#v", handlers)
	}
}

func TestHeavyHandlersIgnoreUnrelatedFrozenContracts(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	root := frozenHeavyContractRoot(t)
	registry, err := jobruntime.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(registry.Profile("heavy")); got <= len(compiledHeavyHandlerKinds) {
		t.Fatalf("fixture has %d heavy descriptors, want more than %d", got, len(compiledHeavyHandlerKinds))
	}

	handlers := compiledWorkerHandlersFromRoot("heavy", root)
	if len(handlers) != len(compiledHeavyHandlerKinds) {
		t.Fatalf("compiled heavy handlers = %d, want %d: %#v", len(handlers), len(compiledHeavyHandlerKinds), handlers)
	}
	for index, handler := range handlers {
		if handler.Kind != compiledHeavyHandlerKinds[index] {
			t.Fatalf("compiled handler %d = %q, want %q", index, handler.Kind, compiledHeavyHandlerKinds[index])
		}
	}
}

func TestHeavyProfileComposesMultipleBuilderFamilies(t *testing.T) {
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
	sources.compiledHandlers = func(string) []jobruntime.HandlerSpec {
		return runtimeRegistry.Profile("heavy")
	}
	reportKinds := map[string]bool{
		jobcontract.KindReportExecuteOnDemand:  true,
		jobcontract.KindReportExecuteScheduled: true,
	}
	dailyKinds := map[string]bool{
		jobcontract.KindDailyMetricsDispatch:  true,
		jobcontract.KindDailyMetricsPartition: true,
		jobcontract.KindDailyMetricsFinalize:  true,
	}
	sources.buildOperational = fakeHandlerBuilder(
		"reports", selectSpecs(runtimeRegistry.Profile("heavy"), reportKinds),
	)
	sources.buildDaily = fakeHandlerBuilder(
		"daily", selectSpecs(runtimeRegistry.Profile("heavy"), dailyKinds),
	)

	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "heavy", RiverDatabaseSchema: "river"},
		health.NewRegistry(time.Second),
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 3 || components[1].Name() != "reports" ||
		components[2].Name() != "daily" {
		t.Fatalf("composed components = %#v", components)
	}
}

func TestHeavyProfileRejectsDuplicateOrMissingBuilderHandlers(t *testing.T) {
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
			sources.compiledHandlers = func(string) []jobruntime.HandlerSpec {
				return runtimeRegistry.Profile("heavy")
			}
			sources.buildOperational = fakeHandlerBuilder(
				"first", selectNamedSpecs(runtimeRegistry, test.first),
			)
			sources.buildDaily = fakeHandlerBuilder(
				"second", selectNamedSpecs(runtimeRegistry, test.second),
			)
			_, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{Profile: "heavy", RiverDatabaseSchema: "river"},
				health.NewRegistry(time.Second),
				sources,
			)
			if !errors.Is(err, errWorkerDependencyUnavailable) || !database.closed.Load() {
				t.Fatalf("configure error=%v database_closed=%t", err, database.closed.Load())
			}
		})
	}
}

func TestProductionBuildersConstructDailyWhileReportsRemainDeferred(t *testing.T) {
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
	sources.compiledHandlers = func(string) []jobruntime.HandlerSpec {
		return runtimeRegistry.Profile("heavy")
	}
	components, err := configureWorkerDependenciesWithSources(
		ctx,
		config.Config{
			Profile:                  "heavy",
			RiverDatabaseSchema:      "river",
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
	if len(components) != 2 || components[0].Name() != "postgres-runtime-pools" ||
		components[1].Name() != "river-heavy-metrics-worker" {
		t.Fatalf("production components = %#v", components)
	}
	if err := components[0].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupportedAvailableContractVersionFailsClosed(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshot: riverstore.QueueTelemetrySnapshot{
			Profile: "ops",
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "heartbeat", Kind: "system.heartbeat"},
				{Queue: "retention", Kind: "system.retention_cleanup"},
			},
			Queues: []riverstore.QueueAgeTelemetry{{Queue: "heartbeat"}, {Queue: "retention"}},
		},
		checkErr: riverstore.ErrUnsupportedAvailableContractVersion,
	}}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	sources.newRiverClientID = func() string { return "test-client" }

	registry := health.NewRegistry(100 * time.Millisecond)
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
		registry,
		sources,
	); err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	want := []string{"profile_completeness", "queued_contract_versions"}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestQueueTelemetryFailureMakesMetricsUnavailable(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{
		snapshotErr: errors.New("postgresql://queue:secret@db/app"),
	}}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	sources.newRiverClientID = func() string { return "test-client" }

	registry := health.NewRegistry(100 * time.Millisecond)
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
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

func TestMissingContractArtifactsFailRegistryAndProfileChecks(t *testing.T) {
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.contractRoot = filepath.Join(t.TempDir(), "missing-contracts")

	registry := health.NewRegistry(100 * time.Millisecond)
	_, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
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
	want := []string{"job_registry", "profile_completeness"}
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
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
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
	domainErr        error
	queueErr         error
	schemaErr        error
	domainSaturation float64
	queueSaturation  float64
	telemetry        queueTelemetrySampler
	telemetryErr     error
	telemetryConfig  riverstore.QueueTelemetryConfig
	closed           atomic.Bool
}

type namedComponent string

func (component namedComponent) Name() string         { return string(component) }
func (namedComponent) Start(context.Context) error    { return nil }
func (namedComponent) Shutdown(context.Context) error { return nil }

func fakeHandlerBuilder(
	name string,
	specs []jobruntime.HandlerSpec,
) func(
	config.Config,
	workerDatabase,
	*jobruntime.Registry,
	jobruntime.Observer,
	*slog.Logger,
) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
	return func(
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
	) (lifecycle.Component, []jobruntime.HandlerSpec, error) {
		return namedComponent(name), specs, nil
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
	for _, job := range document.Jobs {
		kind, _ := job["kind"].(string)
		if strings.HasPrefix(kind, "metrics.daily_") ||
			(promoteReports && strings.HasPrefix(kind, "report.execute_")) {
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

func frozenHeavyContractRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "v1")
	if err := os.CopyFS(root, os.DirFS(defaultContractRoot)); err != nil {
		t.Fatal(err)
	}
	compiled := make(map[string]struct{}, len(compiledHeavyHandlerKinds))
	for _, kind := range compiledHeavyHandlerKinds {
		compiled[kind] = struct{}{}
	}

	registryPath := filepath.Join(root, "registry.json")
	registryData, err := os.ReadFile(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	var contracts jobcontract.Registry
	if err := json.Unmarshal(registryData, &contracts); err != nil {
		t.Fatal(err)
	}
	for index := range contracts.Jobs {
		if _, ok := compiled[contracts.Jobs[index].Kind]; !ok {
			contracts.Jobs[index].Profile = "heavy"
		}
	}
	registryData, err = json.Marshal(contracts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registryPath, registryData, 0o600); err != nil {
		t.Fatal(err)
	}

	migrationPath := filepath.Join(root, "migration-state.json")
	migrationData, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	var migration jobcontract.MigrationState
	if err := json.Unmarshal(migrationData, &migration); err != nil {
		t.Fatal(err)
	}
	for index := range migration.Jobs {
		if _, ok := compiled[migration.Jobs[index].Kind]; !ok {
			migration.Jobs[index].State = "contract_frozen"
			migration.Jobs[index].Route = "celery"
			migration.Jobs[index].RollbackRoute = "celery"
			migration.Jobs[index].RequiredProfiles = []string{"heavy"}
		}
	}
	migrationData, err = json.Marshal(migration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(migrationPath, migrationData, 0o600); err != nil {
		t.Fatal(err)
	}
	return root
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
	snapshot := riverstore.QueueTelemetrySnapshot{Profile: config.Profile}
	for _, job := range config.Jobs {
		snapshot.Jobs = append(snapshot.Jobs, riverstore.QueueJobTelemetry{Queue: job.Queue, Kind: job.Kind})
	}
	for _, queue := range config.Queues {
		snapshot.Queues = append(snapshot.Queues, riverstore.QueueAgeTelemetry{Queue: queue.Name})
	}
	return &fakeQueueTelemetry{snapshot: snapshot}, nil
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
