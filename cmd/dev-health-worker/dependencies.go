package main

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

const (
	defaultContractRoot      = "contracts/jobs/v1"
	defaultDeploymentProfile = "deploy/go-workers/profiles.json"
)

var errWorkerDependencyUnavailable = errors.New("worker readiness dependency is unavailable")

type workerDatabase interface {
	DomainReady(context.Context) error
	QueueReady(context.Context) error
	RiverSchemaReady(context.Context, string) error
	PoolSaturation() (domain float64, queueControl float64)
	NewQueueTelemetrySampler(riverstore.QueueTelemetryConfig) (queueTelemetrySampler, error)
	Close()
}

type queueTelemetrySampler interface {
	Snapshot(context.Context) (riverstore.QueueTelemetrySnapshot, error)
	CheckAvailableContractVersions(context.Context) error
}

type postgresWorkerDatabase struct {
	pools       *postgres.RuntimePools
	domainRole  string
	queueRole   string
	riverSchema string
}

func openWorkerDatabase(ctx context.Context, cfg config.Config) (workerDatabase, error) {
	runtimeConfig := postgres.RuntimeConfigFromPlatform(cfg)
	pools, err := postgres.NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	return &postgresWorkerDatabase{
		pools: pools, domainRole: runtimeConfig.DomainRole, queueRole: runtimeConfig.QueueRole, riverSchema: runtimeConfig.RiverSchema,
	}, nil
}

func (database *postgresWorkerDatabase) DomainReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.Domain == nil {
		return errWorkerDependencyUnavailable
	}
	return postgres.CheckDomainAuthorization(ctx, database.pools.Domain, database.domainRole, database.riverSchema)
}

func (database *postgresWorkerDatabase) QueueReady(ctx context.Context) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errWorkerDependencyUnavailable
	}
	return postgres.CheckQueueAuthorization(ctx, database.pools.QueueControl, database.queueRole, database.riverSchema)
}

func (database *postgresWorkerDatabase) RiverSchemaReady(ctx context.Context, schema string) error {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return errWorkerDependencyUnavailable
	}
	_, err := riverstore.CheckSchema(ctx, database.pools.QueueControl, schema, nil)
	return err
}

func (database *postgresWorkerDatabase) PoolSaturation() (float64, float64) {
	if database == nil || database.pools == nil {
		return 0, 0
	}
	return poolSaturation(database.pools.Domain), poolSaturation(database.pools.QueueControl)
}

func (database *postgresWorkerDatabase) NewQueueTelemetrySampler(
	config riverstore.QueueTelemetryConfig,
) (queueTelemetrySampler, error) {
	if database == nil || database.pools == nil || database.pools.QueueControl == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return riverstore.NewQueueTelemetrySampler(database.pools.QueueControl, config)
}

func poolSaturation(pool *pgxpool.Pool) float64 {
	if pool == nil {
		return 0
	}
	statistics := pool.Stat()
	if statistics == nil || statistics.MaxConns() <= 0 {
		return 0
	}
	return float64(statistics.AcquiredConns()) / float64(statistics.MaxConns())
}

func (database *postgresWorkerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

type workerDependencySources struct {
	openDatabase         func(context.Context, config.Config) (workerDatabase, error)
	loadRuntimeRegistry  func(string) (*jobruntime.Registry, error)
	loadJobRegistry      func(string) (jobcontract.Registry, error)
	loadDeployment       func(string, jobcontract.Registry) (deploymentcontract.Manifest, deploymentcontract.BudgetSummary, error)
	compiledHandlers     func(string) []jobruntime.HandlerSpec
	newRiverClientID     func() string
	buildOperational     func(config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger) (lifecycle.Component, []jobruntime.HandlerSpec, error)
	buildSyncCoordinator func(config.Config, workerDatabase, *slog.Logger) (lifecycle.Component, error)
	buildDaily           func(config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger) (lifecycle.Component, []jobruntime.HandlerSpec, error)
	buildProviderSync    func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger) (lifecycle.Component, []jobruntime.HandlerSpec, error)
	contractRoot         string
	deploymentProfile    string
}

var productionWorkerDependencySources = workerDependencySources{
	openDatabase:         openWorkerDatabase,
	loadRuntimeRegistry:  jobruntime.Load,
	loadJobRegistry:      jobcontract.LoadRegistry,
	loadDeployment:       deploymentcontract.Load,
	compiledHandlers:     compiledWorkerHandlers,
	newRiverClientID:     defaultRiverClientID,
	buildOperational:     buildOperationalWorker,
	buildSyncCoordinator: buildSyncCoordinatorWorker,
	buildDaily:           buildDailyWorker,
	buildProviderSync:    buildProviderSyncWorker,
	contractRoot:         defaultContractRoot,
	deploymentProfile:    defaultDeploymentProfile,
}

func defaultRiverClientID() string {
	return (&river.Config{}).WithDefaults().ID
}

// compiledWorkerHandlers advertises code capability independently of routing.
// Report, daily, and remaining-metrics adapters are complete for the disabled
// heavy profile, but their checked-in routes remain Celery and cannot fetch.
func compiledWorkerHandlers(profile string) []jobruntime.HandlerSpec {
	return compiledWorkerHandlersFromRoot(profile, defaultContractRoot)
}

var compiledHeavyHandlerKinds = []string{
	jobcontract.KindDailyMetricsDispatch,
	jobcontract.KindDailyMetricsFinalize,
	jobcontract.KindDailyMetricsPartition,
	jobcontract.KindRemainingCapacity,
	jobcontract.KindRemainingComplexity,
	jobcontract.KindRemainingDORA,
	jobcontract.KindRemainingExtraMetrics,
	jobcontract.KindRemainingMembership,
	jobcontract.KindRemainingRecommendations,
	jobcontract.KindRemainingReleaseImpact,
	jobcontract.KindRemainingTeamMetrics,
	jobcontract.KindReportExecuteOnDemand,
	jobcontract.KindReportExecuteScheduled,
}

var compiledHandlerKinds = map[string][]string{
	"heavy": compiledHeavyHandlerKinds,
	"sync":  {jobcontract.KindTeamAutoimport},
}

func compiledWorkerHandlersFromRoot(profile, contractRoot string) []jobruntime.HandlerSpec {
	kinds := compiledHandlerKinds[profile]
	if len(kinds) == 0 {
		return nil
	}
	registry, err := jobruntime.Load(contractRoot)
	if err != nil {
		return nil
	}
	handlers := make([]jobruntime.HandlerSpec, 0, len(kinds))
	for _, kind := range kinds {
		handler, ok := registry.Descriptor(kind)
		if !ok || handler.Profile != profile ||
			handler.MigrationState != "go_implemented" || handler.Route != "celery" ||
			handler.RollbackRoute != "celery" {
			return nil
		}
		handlers = append(handlers, handler)
	}
	return handlers
}

type workerDependencies struct {
	database    workerDatabase
	databaseErr error

	runtimeRegistry        *jobruntime.Registry
	registryErr            error
	startup                jobruntime.StartupSpec
	startupErr             error
	metrics                *jobruntime.MetricsCollector
	metricsErr             error
	queueTelemetry         queueTelemetrySampler
	queueTelemetryErr      error
	queueTelemetryRequired bool
}

func configureWorkerDependencies(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
) ([]lifecycle.Component, error) {
	return configureWorkerDependenciesWithLogger(ctx, cfg, registry, slog.Default())
}

func configureWorkerDependenciesWithLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureWorkerDependenciesWithSources(
		ctx,
		cfg,
		registry,
		productionWorkerDependencySources,
		logger,
	)
}

func configureWorkerDependenciesWithSources(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	sources workerDependencySources,
	loggers ...*slog.Logger,
) ([]lifecycle.Component, error) {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	dependencies := buildWorkerDependencies(ctx, cfg, sources)
	if registry == nil {
		dependencies.close()
		return nil, errWorkerDependencyUnavailable
	}
	if dependencies.metricsErr != nil || dependencies.metrics == nil {
		dependencies.close()
		return nil, errWorkerDependencyUnavailable
	}
	if err := registry.RegisterMetrics("worker_runtime", workerMetricsSource{
		collector:              dependencies.metrics,
		database:               dependencies.database,
		queueTelemetry:         dependencies.queueTelemetry,
		queueTelemetryRequired: dependencies.queueTelemetryRequired,
	}); err != nil {
		dependencies.close()
		return nil, err
	}
	providerRuntimeConstructed := false
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "job_registry", check: dependencies.jobRegistryReady},
		{name: "profile_completeness", check: dependencies.profileReady},
		{name: "provider_route_switches", check: providerRouteSwitchesReady(cfg, &providerRuntimeConstructed)},
		{name: "queued_contract_versions", check: dependencies.queuedContractVersionsReady},
		{name: "queue_control_config", check: dependencies.queueControlConfigReady},
		{name: "queue_postgres", check: dependencies.queueReady},
		{name: "river_schema", check: dependencies.riverSchemaReady(cfg.RiverDatabaseSchema)},
	}
	for _, check := range checks {
		if err := registry.RegisterRequired(check.name, check.check); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	if dependencies.database == nil {
		return nil, nil
	}
	components := []lifecycle.Component{workerDatabaseLifecycle{database: dependencies.database}}
	var activeHandlers []jobruntime.HandlerSpec
	for _, build := range []func(
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
	) (lifecycle.Component, []jobruntime.HandlerSpec, error){
		sources.buildOperational,
		sources.buildDaily,
	} {
		if build == nil {
			continue
		}
		component, handlers, err := build(
			cfg, dependencies.database, dependencies.runtimeRegistry, dependencies.metrics, logger,
		)
		if err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
		activeHandlers, err = composeHandlerSpecs(activeHandlers, handlers)
		if err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
		if component != nil {
			components = append(components, component)
		}
	}
	if len(activeHandlers) > 0 {
		dependencies.startup.Handlers = activeHandlers
		if err := dependencies.profileReady(ctx); err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
	}
	if sources.buildSyncCoordinator != nil {
		component, err := sources.buildSyncCoordinator(cfg, dependencies.database, logger)
		if err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
		if component != nil {
			components = append(components, component)
		}
	}
	if sources.buildProviderSync != nil {
		component, handlers, err := sources.buildProviderSync(
			ctx, cfg, dependencies.database, dependencies.runtimeRegistry,
			dependencies.metrics, logger,
		)
		if err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
		if len(handlers) > 0 {
			dependencies.startup.Handlers = handlers
			for _, handler := range handlers {
				if handler.Kind == jobcontract.KindSyncProviderUnit {
					providerRuntimeConstructed = true
				}
			}
		}
		if component != nil {
			components = append(components, component)
		}
	}
	return components, nil
}

func composeHandlerSpecs(
	existing []jobruntime.HandlerSpec,
	additional []jobruntime.HandlerSpec,
) ([]jobruntime.HandlerSpec, error) {
	result := append([]jobruntime.HandlerSpec(nil), existing...)
	seen := make(map[string]struct{}, len(existing)+len(additional))
	for _, handler := range existing {
		if handler.Kind == "" {
			return nil, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return nil, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
	}
	for _, handler := range additional {
		if handler.Kind == "" {
			return nil, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return nil, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
		result = append(result, handler)
	}
	return result, nil
}

func providerRouteSwitchesReady(
	cfg config.Config,
	runtimeConstructed *bool,
) health.CheckFunc {
	switches := providersync.CompleteRouteSwitches{
		LinearWorkItems:          cfg.WorkerLinearWorkItemsEnabled,
		JiraWorkItems:            cfg.WorkerJiraWorkItemsEnabled,
		JiraIncidents:            cfg.WorkerJiraIncidentsEnabled,
		LaunchDarklyFeatureFlags: cfg.WorkerLaunchDarklyFeatureFlagsEnabled,
	}
	routes := []struct {
		provider string
		dataset  string
		enabled  bool
	}{
		{"linear", "work-items", cfg.WorkerLinearWorkItemsEnabled},
		{"jira", "work-items", cfg.WorkerJiraWorkItemsEnabled},
		{"jira", "incidents", cfg.WorkerJiraIncidentsEnabled},
		{"launchdarkly", "feature-flags", cfg.WorkerLaunchDarklyFeatureFlagsEnabled},
	}
	return func(context.Context) error {
		for _, route := range routes {
			if !route.enabled {
				continue
			}
			descriptor, ok := switches.Descriptor(route.provider, route.dataset)
			if !ok || !descriptor.RouteReady || !descriptor.RouteEnabled {
				return errWorkerDependencyUnavailable
			}
			if route.provider == "launchdarkly" &&
				(runtimeConstructed == nil || !*runtimeConstructed) {
				return errWorkerDependencyUnavailable
			}
		}
		return nil
	}
}

type workerMetricsSource struct {
	collector              *jobruntime.MetricsCollector
	database               workerDatabase
	queueTelemetry         queueTelemetrySampler
	queueTelemetryRequired bool
}

func (source workerMetricsSource) WritePrometheus(output io.Writer) error {
	if source.collector == nil {
		return errWorkerDependencyUnavailable
	}
	if source.database != nil {
		domain, queueControl := source.database.PoolSaturation()
		if err := source.collector.SetDatabasePoolSaturation("domain", domain); err != nil {
			return err
		}
		if err := source.collector.SetDatabasePoolSaturation("queue_control", queueControl); err != nil {
			return err
		}
	}
	if source.queueTelemetryRequired {
		if source.queueTelemetry == nil {
			return errWorkerDependencyUnavailable
		}
		snapshot, err := source.queueTelemetry.Snapshot(context.Background())
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		for _, job := range snapshot.Jobs {
			if err := source.collector.SetJobsAvailable(jobruntime.JobLabels{
				Profile: snapshot.Profile,
				Queue:   job.Queue,
				Kind:    job.Kind,
			}, job.Available); err != nil {
				return err
			}
		}
		for _, queue := range snapshot.Queues {
			if err := source.collector.SetJobOldestAge(snapshot.Profile, queue.Queue, queue.OldestAvailableAge); err != nil {
				return err
			}
		}
		if err := source.collector.SetExecutionSaturation(snapshot.Profile, snapshot.ExecutionSaturation); err != nil {
			return err
		}
	}
	return source.collector.WritePrometheus(output)
}

func buildWorkerDependencies(
	ctx context.Context,
	cfg config.Config,
	sources workerDependencySources,
) *workerDependencies {
	dependencies := &workerDependencies{}
	if sources.openDatabase == nil {
		dependencies.databaseErr = errWorkerDependencyUnavailable
	} else {
		dependencies.database, dependencies.databaseErr = sources.openDatabase(ctx, cfg)
		if dependencies.databaseErr != nil && dependencies.database != nil {
			dependencies.database.Close()
			dependencies.database = nil
		}
	}

	if sources.loadRuntimeRegistry == nil || sources.contractRoot == "" {
		dependencies.registryErr = errWorkerDependencyUnavailable
		dependencies.startupErr = errWorkerDependencyUnavailable
		dependencies.metrics, dependencies.metricsErr = buildWorkerMetrics(ctx, cfg, nil)
		return dependencies
	}
	dependencies.runtimeRegistry, dependencies.registryErr = sources.loadRuntimeRegistry(sources.contractRoot)
	dependencies.metrics, dependencies.metricsErr = buildWorkerMetrics(ctx, cfg, dependencies.runtimeRegistry)
	if dependencies.registryErr != nil {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	if sources.loadJobRegistry == nil || sources.loadDeployment == nil || sources.compiledHandlers == nil || sources.deploymentProfile == "" {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	contracts, err := sources.loadJobRegistry(sources.contractRoot)
	if err != nil {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	manifest, _, err := sources.loadDeployment(sources.deploymentProfile, contracts)
	if err != nil {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	process, ok := riverProcessForProfile(manifest, cfg.Profile)
	if !ok {
		dependencies.startupErr = errWorkerDependencyUnavailable
		return dependencies
	}
	dependencies.startup = jobruntime.StartupSpec{
		Profile:  cfg.Profile,
		Queues:   append([]string(nil), process.Queues...),
		Handlers: sources.compiledHandlers(cfg.Profile),
	}
	dependencies.buildQueueTelemetry(cfg, process, sources)
	return dependencies
}

func (dependencies *workerDependencies) buildQueueTelemetry(
	cfg config.Config,
	process deploymentcontract.Process,
	sources workerDependencySources,
) {
	descriptors := dependencies.runtimeRegistry.Profile(cfg.Profile)
	if len(descriptors) == 0 || len(process.Queues) == 0 {
		return
	}
	dependencies.queueTelemetryRequired = true
	if dependencies.databaseErr != nil || dependencies.database == nil || sources.newRiverClientID == nil {
		dependencies.queueTelemetryErr = errWorkerDependencyUnavailable
		return
	}
	queues := make([]riverstore.QueueTelemetryQueue, 0, len(process.QueueWorkers))
	for _, queue := range process.QueueWorkers {
		queues = append(queues, riverstore.QueueTelemetryQueue{Name: queue.Queue, MaxWorkers: queue.MaxWorkers})
	}
	jobs := make([]riverstore.QueueTelemetryJob, 0, len(descriptors))
	for _, descriptor := range descriptors {
		jobs = append(jobs, riverstore.QueueTelemetryJob{
			Queue:             descriptor.Queue,
			Kind:              descriptor.Kind,
			SupportedVersions: append([]int(nil), descriptor.SupportedVersions...),
		})
	}
	dependencies.queueTelemetry, dependencies.queueTelemetryErr = dependencies.database.NewQueueTelemetrySampler(
		riverstore.QueueTelemetryConfig{
			Schema:   cfg.RiverDatabaseSchema,
			Profile:  cfg.Profile,
			ClientID: sources.newRiverClientID(),
			Queues:   queues,
			Jobs:     jobs,
		},
	)
}

func buildWorkerMetrics(
	ctx context.Context,
	cfg config.Config,
	runtimeRegistry *jobruntime.Registry,
) (*jobruntime.MetricsCollector, error) {
	dimensions := jobruntime.MetricDimensions{Profiles: []string{cfg.Profile}}
	if runtimeRegistry != nil && runtimeRegistry.HasProfile(cfg.Profile) {
		derived, err := jobruntime.DimensionsForProfile(runtimeRegistry, cfg.Profile, nil, nil)
		if err != nil {
			return nil, err
		}
		dimensions = derived
	}
	collector, err := jobruntime.NewMetricsCollector(dimensions)
	if err != nil {
		return nil, err
	}
	build := version.Current(cfg.Service)
	if err := jobruntime.RegisterRuntime(ctx, collector, jobruntime.RuntimeInfo{
		Version: build.Version,
		Commit:  build.Commit,
		Profile: cfg.Profile,
	}); err != nil {
		return nil, err
	}
	return collector, nil
}

func riverProcessForProfile(manifest deploymentcontract.Manifest, profile string) (deploymentcontract.Process, bool) {
	for _, process := range manifest.Processes {
		if process.Runtime == "river" && process.RegistryProfile != nil && *process.RegistryProfile == profile {
			return process, true
		}
	}
	return deploymentcontract.Process{}, false
}

func (dependencies *workerDependencies) domainReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.database.DomainReady(ctx); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (dependencies *workerDependencies) queueReady(ctx context.Context) error {
	if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.database.QueueReady(ctx); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

// queueControlConfigReady gives operators one bounded, actionable readiness
// category for queue-control configuration failures. The underlying error is
// never exposed because it may have originated at a DSN parsing boundary.
// Connectivity and schema failures remain separate readiness categories.
func (dependencies *workerDependencies) queueControlConfigReady(context.Context) error {
	if dependencies == nil {
		return errWorkerDependencyUnavailable
	}
	if dependencies.databaseErr == nil {
		return nil
	}
	for _, configurationError := range []error{
		postgres.ErrQueueControlRequired,
		postgres.ErrQueueControlTransactionMode,
		postgres.ErrQueueControlSessionUnverified,
		postgres.ErrRuntimeRolesNotSeparated,
		postgres.ErrRuntimeRoleConfiguration,
	} {
		if errors.Is(dependencies.databaseErr, configurationError) {
			return errWorkerDependencyUnavailable
		}
	}
	return nil
}

func (dependencies *workerDependencies) riverSchemaReady(schema string) health.CheckFunc {
	return func(ctx context.Context) error {
		if dependencies == nil || dependencies.databaseErr != nil || dependencies.database == nil {
			return errWorkerDependencyUnavailable
		}
		if err := dependencies.database.RiverSchemaReady(ctx, schema); err != nil {
			return errWorkerDependencyUnavailable
		}
		return nil
	}
}

func (dependencies *workerDependencies) jobRegistryReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil || dependencies.runtimeRegistry == nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (dependencies *workerDependencies) profileReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil || dependencies.runtimeRegistry == nil || dependencies.startupErr != nil {
		return errWorkerDependencyUnavailable
	}
	expected := make(map[string]jobruntime.Descriptor)
	for _, descriptor := range dependencies.runtimeRegistry.Profile(dependencies.startup.Profile) {
		if descriptor.Executable() {
			expected[descriptor.Kind] = descriptor
		}
	}
	if len(expected) == 0 || len(dependencies.startup.Handlers) != len(expected) {
		return errWorkerDependencyUnavailable
	}
	seen := make(map[string]struct{}, len(dependencies.startup.Handlers))
	for _, handler := range dependencies.startup.Handlers {
		descriptor, ok := expected[handler.Kind]
		if !ok || descriptor.Kind == "" {
			return errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
		if err := dependencies.runtimeRegistry.ValidateHandler(handler); err != nil {
			return errWorkerDependencyUnavailable
		}
	}
	for kind := range expected {
		if _, ok := seen[kind]; !ok {
			return errWorkerDependencyUnavailable
		}
	}
	return nil
}

func (dependencies *workerDependencies) queuedContractVersionsReady(ctx context.Context) error {
	if dependencies == nil || !dependencies.queueTelemetryRequired {
		return nil
	}
	if dependencies.queueTelemetryErr != nil || dependencies.queueTelemetry == nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.queueTelemetry.CheckAvailableContractVersions(ctx); err != nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (dependencies *workerDependencies) close() {
	if dependencies != nil && dependencies.database != nil {
		dependencies.database.Close()
	}
}

type workerDatabaseLifecycle struct {
	database workerDatabase
}

func (workerDatabaseLifecycle) Name() string { return "postgres-runtime-pools" }

func (component workerDatabaseLifecycle) Start(context.Context) error {
	if component.database == nil {
		return errWorkerDependencyUnavailable
	}
	return nil
}

func (component workerDatabaseLifecycle) Shutdown(context.Context) error {
	if component.database != nil {
		component.database.Close()
	}
	return nil
}
