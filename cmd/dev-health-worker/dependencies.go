package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

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
	// AttachPoolAcquireObserver wires worker_database_pool_acquire_seconds
	// into the domain/queue-control pools' Acquire path. It is called once
	// dependencies.metrics exists, which is after the database itself opens
	// (see NewRuntimePools's tracer freeze-at-construction constraint).
	AttachPoolAcquireObserver(postgres.PoolAcquireObserver)
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

// githubProjectsV2DurableConfigReader is deliberately narrower than
// workerDatabase: Projects v2 configuration is a startup advisory, not a
// per-claim collector dependency. Production obtains it from the domain pool
// only after CheckDomainAuthorization has proven the 0088 snapshot-table
// posture; tests can provide the same small query seam without widening roles.
type githubProjectsV2DurableConfigReader interface {
	GitHubProjectsV2Configured(context.Context) (bool, error)
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

// GitHubProjectsV2Configured reports whether any enabled GitHub integration
// has the durable D18 target key. Presence is intentional: an explicit empty
// list is durable configuration (and means no Projects v2 targets), while a
// malformed value is handled later as ErrInvalidConfiguration for its claim.
// This is a startup-only census, never a route/collector environment fallback.
func (database *postgresWorkerDatabase) GitHubProjectsV2Configured(ctx context.Context) (bool, error) {
	if database == nil || database.pools == nil || database.pools.Domain == nil || ctx == nil {
		return false, errWorkerDependencyUnavailable
	}
	const query = `
SELECT EXISTS (
	SELECT 1
	FROM public.integrations
	WHERE lower(provider) = 'github'
		AND is_active
		AND config::jsonb ? 'github_projects_v2'
)`
	var configured bool
	if err := database.pools.Domain.QueryRow(ctx, query).Scan(&configured); err != nil {
		return false, err
	}
	return configured, nil
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

func (database *postgresWorkerDatabase) AttachPoolAcquireObserver(observer postgres.PoolAcquireObserver) {
	if database == nil || database.pools == nil {
		return
	}
	database.pools.AttachPoolAcquireObserver(observer)
}

func (database *postgresWorkerDatabase) Close() {
	if database != nil && database.pools != nil {
		database.pools.Close()
	}
}

// workerFamily is everything one constructed handler family contributes to the
// process: its lifecycle component, the adapters it concretely registered, and
// the River queue budget it actually consumes. Runtime capability is only ever
// what a builder constructed here — never what a compiled list advertises.
type workerFamily struct {
	component lifecycle.Component
	handlers  []jobruntime.HandlerSpec
	queues    []jobruntime.QueueBudget
	// metricsSource is an additional Prometheus fragment this family owns
	// (e.g. providerfoundation's dev_health_provider_* family), registered
	// with the health.Registry alongside "worker_runtime" once construction
	// finishes. Most families leave this nil.
	metricsSource health.MetricsSource
}

type workerFamilyBuilder func(
	config.Config,
	workerDatabase,
	*jobruntime.Registry,
	jobruntime.Observer,
	*slog.Logger,
) (workerFamily, error)

type workerDependencySources struct {
	openDatabase         func(context.Context, config.Config) (workerDatabase, error)
	loadRuntimeRegistry  func(string) (*jobruntime.Registry, error)
	loadJobRegistry      func(string) (jobcontract.Registry, error)
	loadDeployment       func(string, jobcontract.Registry) (deploymentcontract.Manifest, deploymentcontract.BudgetSummary, error)
	newRiverClientID     func() string
	buildOperational     workerFamilyBuilder
	buildSyncCoordinator workerFamilyBuilder
	buildDaily           workerFamilyBuilder
	buildReports         func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger) (workerFamily, error)
	buildProviderSync    func(context.Context, config.Config, workerDatabase, *jobruntime.Registry, jobruntime.Observer, *slog.Logger) (workerFamily, error)
	buildWorkgraph       workerFamilyBuilder
	contractRoot         string
	deploymentProfile    string
}

var productionWorkerDependencySources = workerDependencySources{
	openDatabase:         openWorkerDatabase,
	loadRuntimeRegistry:  jobruntime.Load,
	loadJobRegistry:      jobcontract.LoadRegistry,
	loadDeployment:       deploymentcontract.Load,
	newRiverClientID:     defaultRiverClientID,
	buildOperational:     buildOperationalWorker,
	buildSyncCoordinator: buildSyncCoordinatorWorker,
	buildDaily:           buildDailyWorker,
	buildReports:         buildReportWorker,
	buildProviderSync:    buildProviderSyncWorker,
	buildWorkgraph:       buildWorkgraphWorker,
	contractRoot:         defaultContractRoot,
	deploymentProfile:    defaultDeploymentProfile,
}

func defaultRiverClientID() string {
	return (&river.Config{}).WithDefaults().ID
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
	// worker_database_pool_acquire_seconds needs the collector, which is why
	// this happens here rather than at pool construction: NewRuntimePools
	// freezes its pgxpool tracer before dependencies.metrics exists.
	if dependencies.database != nil {
		dependencies.database.AttachPoolAcquireObserver(dependencies.metrics)
	}
	providerRuntimeConstructed := false
	checks := []struct {
		name  string
		check health.CheckFunc
	}{
		{name: "domain_postgres", check: dependencies.domainReady},
		{name: "github_projects_v2_startup_config", check: githubProjectsV2StartupReadiness(dependencies.database, logger)},
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
	// Native replacement for the monitor-queue-depths Beat task. It reads the
	// same River telemetry the readiness and metrics paths use, so queue depth
	// and age come from the authoritative queue-control tables.
	if monitor := newQueueHealthMonitor(
		dependencies.queueTelemetry, logger, cfg.Profile,
	); monitor != nil {
		components = append(components, monitor)
	}
	var active workerFamily
	build := func(family workerFamily, err error) error {
		if err != nil {
			return err
		}
		active, err = composeWorkerFamily(active, family)
		if err != nil {
			return err
		}
		if family.component != nil {
			components = append(components, family.component)
		}
		return nil
	}
	for _, builder := range []workerFamilyBuilder{
		sources.buildOperational,
		sources.buildDaily,
		sources.buildWorkgraph,
		sources.buildSyncCoordinator,
	} {
		if builder == nil {
			continue
		}
		if err := build(builder(
			cfg, dependencies.database, dependencies.runtimeRegistry, dependencies.metrics, logger,
		)); err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
	}
	for _, builder := range []func(
		context.Context,
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
	) (workerFamily, error){
		sources.buildReports,
		sources.buildProviderSync,
	} {
		if builder == nil {
			continue
		}
		if err := build(builder(
			ctx, cfg, dependencies.database, dependencies.runtimeRegistry,
			dependencies.metrics, logger,
		)); err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
	}
	if active.metricsSource != nil {
		if err := registry.RegisterMetrics("provider_foundation", active.metricsSource); err != nil {
			dependencies.close()
			return nil, err
		}
	}
	for _, handler := range active.handlers {
		if handler.Kind == jobcontract.KindSyncProviderUnit {
			providerRuntimeConstructed = true
		}
	}
	// Constructed capability is the only capability. Publish it before the
	// readiness gate opens so exact startup validation sees what this binary
	// actually built, then refuse to start when it does not cover the profile.
	dependencies.startup.Handlers = active.handlers
	dependencies.startup.Queues = active.queues
	if len(active.handlers) > 0 || len(active.queues) > 0 {
		if err := dependencies.profileReady(ctx); err != nil {
			dependencies.close()
			return nil, errWorkerDependencyUnavailable
		}
	}
	return components, nil
}

// githubProjectsV2StartupReadiness implements CHAOS-3506's one warning-only
// D18 bridge. It first executes the existing domain authorization check (which
// proves migration 0088's snapshot-table posture without selecting Alembic
// state), then asks whether any enabled integration owns the durable target
// key. The environment value itself is never logged or read by the hot route.
func githubProjectsV2StartupReadiness(
	database workerDatabase,
	logger *slog.Logger,
) health.CheckFunc {
	if logger == nil {
		logger = slog.Default()
	}
	var warned sync.Once
	return func(ctx context.Context) error {
		orphaned, err := githubProjectsV2EnvironmentNeedsStartupWarning(
			ctx,
			func(ctx context.Context) (bool, error) {
				if database == nil {
					return false, errWorkerDependencyUnavailable
				}
				// Keep migration/least-privilege verification ahead of the
				// advisory query. DomainReady intentionally reaches only
				// postgres.CheckDomainAuthorization, never public.alembic_version.
				if err := database.DomainReady(ctx); err != nil {
					return false, errWorkerDependencyUnavailable
				}
				reader, ok := database.(githubProjectsV2DurableConfigReader)
				if !ok {
					return false, errWorkerDependencyUnavailable
				}
				return reader.GitHubProjectsV2Configured(ctx)
			},
		)
		if err != nil {
			return errWorkerDependencyUnavailable
		}
		if orphaned {
			warned.Do(func() {
				logger.Warn(
					"GITHUB_PROJECTS_V2 is set but no enabled GitHub integration has durable github_projects_v2 configuration; Go ignores the environment setting",
					"environment", "GITHUB_PROJECTS_V2",
				)
			})
		}
		return nil
	}
}

func composeWorkerFamily(existing, additional workerFamily) (workerFamily, error) {
	result := workerFamily{
		handlers:      append([]jobruntime.HandlerSpec(nil), existing.handlers...),
		queues:        append([]jobruntime.QueueBudget(nil), existing.queues...),
		metricsSource: existing.metricsSource,
	}
	if additional.metricsSource != nil {
		if result.metricsSource != nil {
			// Two families both claiming an additional metrics fragment would
			// silently drop one of them at registration; fail closed instead.
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		result.metricsSource = additional.metricsSource
	}
	seen := make(map[string]struct{}, len(result.handlers)+len(additional.handlers))
	for _, handler := range result.handlers {
		if handler.Kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
	}
	for _, handler := range additional.handlers {
		if handler.Kind == "" {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if _, duplicate := seen[handler.Kind]; duplicate {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		seen[handler.Kind] = struct{}{}
		result.handlers = append(result.handlers, handler)
	}
	queues := make(map[string]struct{}, len(result.queues)+len(additional.queues))
	for _, queue := range result.queues {
		queues[queue.Queue] = struct{}{}
	}
	for _, queue := range additional.queues {
		if _, duplicate := queues[queue.Queue]; duplicate {
			// Two families cannot both own one queue's worker budget: the
			// deployment manifest budgets each queue exactly once.
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		queues[queue.Queue] = struct{}{}
		result.queues = append(result.queues, queue)
	}
	return result, nil
}

// workerRouteSwitches is the single translation from process configuration to
// route switches. The readiness check below and the handler that actually
// executes claims (buildProviderSyncHandler) both read it, so a route can
// never be reported ready against one switch set while being served under
// another — the drift that a hardcoded `LaunchDarklyFeatureFlags: true` in the
// handler used to permit (CHAOS-3123).
func workerRouteSwitches(cfg config.Config) providersync.CompleteRouteSwitches {
	return providersync.CompleteRouteSwitches{
		LocalAllRoutes:              cfg.LocalAllProviderRoutes,
		LinearWorkItems:             cfg.WorkerLinearWorkItemsEnabled,
		JiraWorkItems:               cfg.WorkerJiraWorkItemsEnabled,
		JiraIncidents:               cfg.WorkerJiraIncidentsEnabled,
		LaunchDarklyFeatureFlags:    cfg.WorkerLaunchDarklyFeatureFlagsEnabled,
		GithubRepoMetadata:          cfg.WorkerGithubRepoMetadataEnabled,
		GitlabRepoMetadata:          cfg.WorkerGitlabRepoMetadataEnabled,
		GitlabCommits:               cfg.WorkerGitlabCommitsEnabled,
		GitlabCommitStats:           cfg.WorkerGitlabCommitStatsEnabled,
		GitlabCICD:                  cfg.WorkerGitlabCICDEnabled,
		GitlabTests:                 cfg.WorkerGitlabTestsEnabled,
		GitlabIncidents:             cfg.WorkerGitlabIncidentsEnabled,
		GitlabDeployments:           cfg.WorkerGitlabDeploymentsEnabled,
		GitlabFeatureFlags:          cfg.WorkerGitlabFeatureFlagsEnabled,
		GitlabFiles:                 cfg.WorkerGitlabFilesEnabled,
		GitlabBlame:                 cfg.WorkerGitlabBlameEnabled,
		GitlabPRs:                   cfg.WorkerGitlabPRsEnabled,
		GitlabPRReviews:             cfg.WorkerGitlabPRReviewsEnabled,
		GitlabPRComments:            cfg.WorkerGitlabPRCommentsEnabled,
		GitlabSecurity:              cfg.WorkerGitlabSecurityEnabled,
		GitlabWorkItems:             cfg.WorkerGitlabWorkItemsEnabled,
		PagerDutyServices:           cfg.WorkerPagerDutyServicesEnabled,
		PagerDutyBusinessServices:   cfg.WorkerPagerDutyBusinessServicesEnabled,
		PagerDutyEscalationPolicies: cfg.WorkerPagerDutyEscalationPoliciesEnabled,
		PagerDutySchedules:          cfg.WorkerPagerDutySchedulesEnabled,
		PagerDutyOnCalls:            cfg.WorkerPagerDutyOnCallsEnabled,
		PagerDutyUsers:              cfg.WorkerPagerDutyUsersEnabled,
		PagerDutyTeams:              cfg.WorkerPagerDutyTeamsEnabled,
		PagerDutyIncidents:          cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentAlerts:     cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentLogEntries: cfg.WorkerPagerDutyIncidentsEnabled,
		PagerDutyIncidentNotes:      cfg.WorkerPagerDutyIncidentsEnabled,
		GithubPRs:                   cfg.WorkerGithubPRsEnabled,
		GithubPRReviews:             cfg.WorkerGithubPRReviewsEnabled,
		GithubPRComments:            cfg.WorkerGithubPRCommentsEnabled,
		GithubCICD:                  cfg.WorkerGithubCICDEnabled,
		GithubCommits:               cfg.WorkerGithubCommitsEnabled,
		GithubDeployments:           cfg.WorkerGithubDeploymentsEnabled,
		GithubSecurity:              cfg.WorkerGithubSecurityEnabled,
		GithubFiles:                 cfg.WorkerGithubFilesEnabled,
		GithubCommitStats:           cfg.WorkerGithubCommitStatsEnabled,
		GithubBlame:                 cfg.WorkerGithubBlameEnabled,
		GithubTests:                 cfg.WorkerGithubTestsEnabled,
		GithubWorkItems:             cfg.WorkerGithubWorkItemsEnabled,
	}
}

type providerRouteSwitch struct {
	provider   string
	dataset    string
	configured bool
}

func effectiveProviderRouteSwitches(cfg config.Config) []providerRouteSwitch {
	switches := workerRouteSwitches(cfg)
	routes := []providerRouteSwitch{
		{"linear", "work-items", cfg.WorkerLinearWorkItemsEnabled},
		{"jira", "work-items", cfg.WorkerJiraWorkItemsEnabled},
		{"jira", "incidents", cfg.WorkerJiraIncidentsEnabled},
		{"launchdarkly", "feature-flags", cfg.WorkerLaunchDarklyFeatureFlagsEnabled},
		{"github", "repo-metadata", cfg.WorkerGithubRepoMetadataEnabled},
		{"gitlab", "repo-metadata", cfg.WorkerGitlabRepoMetadataEnabled},
		{"gitlab", "incidents", cfg.WorkerGitlabIncidentsEnabled},
		{"gitlab", "commits", cfg.WorkerGitlabCommitsEnabled},
		{"gitlab", "commit-stats", cfg.WorkerGitlabCommitStatsEnabled},
		{"gitlab", "cicd", cfg.WorkerGitlabCICDEnabled},
		{"gitlab", "tests", cfg.WorkerGitlabTestsEnabled},
		{"gitlab", "deployments", cfg.WorkerGitlabDeploymentsEnabled},
		{"gitlab", "feature-flags", cfg.WorkerGitlabFeatureFlagsEnabled},
		{"gitlab", "files", cfg.WorkerGitlabFilesEnabled},
		{"gitlab", "blame", cfg.WorkerGitlabBlameEnabled},
		{"gitlab", "prs", cfg.WorkerGitlabPRsEnabled},
		{"gitlab", "pr-reviews", cfg.WorkerGitlabPRReviewsEnabled},
		{"gitlab", "pr-comments", cfg.WorkerGitlabPRCommentsEnabled},
		{"gitlab", "security", cfg.WorkerGitlabSecurityEnabled},
		{"gitlab", "work-items", cfg.WorkerGitlabWorkItemsEnabled},
		{"pagerduty", "services", cfg.WorkerPagerDutyServicesEnabled},
		{"pagerduty", "business-services", cfg.WorkerPagerDutyBusinessServicesEnabled},
		{"pagerduty", "escalation-policies", cfg.WorkerPagerDutyEscalationPoliciesEnabled},
		{"pagerduty", "schedules", cfg.WorkerPagerDutySchedulesEnabled},
		{"pagerduty", "on-calls", cfg.WorkerPagerDutyOnCallsEnabled},
		{"pagerduty", "users", cfg.WorkerPagerDutyUsersEnabled},
		{"pagerduty", "teams", cfg.WorkerPagerDutyTeamsEnabled},
		{"pagerduty", "incidents", cfg.WorkerPagerDutyIncidentsEnabled},
		{"pagerduty", "incident-alerts", cfg.WorkerPagerDutyIncidentsEnabled},
		{"pagerduty", "incident-log-entries", cfg.WorkerPagerDutyIncidentsEnabled},
		{"pagerduty", "incident-notes", cfg.WorkerPagerDutyIncidentsEnabled},
		{"github", "prs", cfg.WorkerGithubPRsEnabled},
		{"github", "pr-reviews", cfg.WorkerGithubPRReviewsEnabled},
		{"github", "pr-comments", cfg.WorkerGithubPRCommentsEnabled},
		{"github", "cicd", cfg.WorkerGithubCICDEnabled},
		{"github", "commits", cfg.WorkerGithubCommitsEnabled},
		{"github", "deployments", cfg.WorkerGithubDeploymentsEnabled},
		{"github", "security", cfg.WorkerGithubSecurityEnabled},
		{"github", "files", cfg.WorkerGithubFilesEnabled},
		{"github", "commit-stats", cfg.WorkerGithubCommitStatsEnabled},
		{"github", "blame", cfg.WorkerGithubBlameEnabled},
		{"github", "tests", cfg.WorkerGithubTestsEnabled},
		{"github", "work-items", cfg.WorkerGithubWorkItemsEnabled},
	}
	effective := make([]providerRouteSwitch, 0, len(routes))
	for _, route := range routes {
		descriptor, ok := switches.Descriptor(route.provider, route.dataset)
		if route.configured || (ok && descriptor.RouteEnabled) {
			effective = append(effective, route)
		}
	}
	return effective
}

func providerRouteSwitchesReady(
	cfg config.Config,
	runtimeConstructed *bool,
) health.CheckFunc {
	switches := workerRouteSwitches(cfg)
	routes := effectiveProviderRouteSwitches(cfg)
	return func(context.Context) error {
		// The same helper feeds the production BuildExecutor closure below.
		// A route cannot report ready for one pair of config files and construct
		// the handler with another, and ambient STATUS_MAPPING_PATH is rejected
		// before either side opens a provider connection (D19).
		if _, err := workItemsRuntimeConfigFrom(cfg); err != nil {
			return errWorkerDependencyUnavailable
		}
		for _, route := range routes {
			descriptor, ok := switches.Descriptor(route.provider, route.dataset)
			if !ok || !descriptor.RouteReady || !descriptor.RouteEnabled {
				return errWorkerDependencyUnavailable
			}
			// Any enabled route implies the provider-sync runtime must have
			// been constructed: buildProviderSyncWorker builds the family when
			// ANY route switch is on, so an enabled switch with no runtime
			// behind it means units are being dispatched at a process that
			// registered no handler for them. This was previously checked for
			// launchdarkly alone, which was correct only while it was the sole
			// switch that could construct the family (CHAOS-3123).
			if runtimeConstructed == nil || !*runtimeConstructed {
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
	if sources.loadJobRegistry == nil || sources.loadDeployment == nil || sources.deploymentProfile == "" {
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
	// Queues and Handlers stay empty here on purpose: they are filled in only by
	// concretely constructed handler families. Everything the manifest declares
	// is an expectation to prove against, never a capability claim.
	manifestQueues := make([]jobruntime.QueueBudget, 0, len(process.QueueWorkers))
	for _, queue := range process.QueueWorkers {
		manifestQueues = append(manifestQueues, jobruntime.QueueBudget{
			Queue: queue.Queue, MaxWorkers: queue.MaxWorkers,
		})
	}
	dependencies.startup = jobruntime.StartupSpec{
		Profile:        cfg.Profile,
		ManifestQueues: manifestQueues,
		Connections: jobruntime.ConnectionBudget{
			QueueControl: int(cfg.QueueDatabaseMaxConns),
			Domain:       int(cfg.DomainDatabaseMaxConns),
		},
		ManifestConnections: jobruntime.ConnectionBudget{
			QueueControl: process.QueueControlMaxConnections,
			Domain:       process.DomainMaxConnections,
		},
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
		derived, err := jobruntime.DimensionsForProfile(
			runtimeRegistry, cfg.Profile, nil,
			budgetDimensionsForProfile(cfg.Profile), syncLeaseDimensionsForProfile(cfg.Profile),
		)
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

// syncLeaseDimensionsForProfile registers the frozen provider/dataset matrix
// (providersync.MatrixProviders + Capabilities, TRD §10.1) as the bounded
// worker_sync_lease_expired_total dimension set. Only the "sync" profile
// constructs the provider-unit handler that can ever observe a recovered
// claim, so other profiles keep an empty, harmless set. The matrix is static
// deployment configuration, not runtime or tenant data, so this stays well
// under maxMetricSyncLeases regardless of which providers are feature-flag
// enabled today.
func syncLeaseDimensionsForProfile(profile string) []jobruntime.SyncLeaseLabels {
	if profile != "sync" {
		return nil
	}
	var labels []jobruntime.SyncLeaseLabels
	for _, provider := range providersync.MatrixProviders() {
		for _, capability := range providersync.Capabilities(provider) {
			labels = append(labels, jobruntime.SyncLeaseLabels{
				Provider: capability.Provider, DatasetFamily: capability.Dataset,
			})
		}
	}
	return labels
}

// budgetDimensionsForProfile registers the same frozen provider matrix, paired
// with providersync's three static cost classes, as the bounded
// worker_budget_wait_seconds dimension set. Only "sync" builds a provider-unit
// executor that ever acquires a provider cost budget.
func budgetDimensionsForProfile(profile string) []jobruntime.BudgetLabels {
	if profile != "sync" {
		return nil
	}
	costClasses := []providersync.CostClass{
		providersync.CostLight, providersync.CostMedium, providersync.CostHeavy,
	}
	var labels []jobruntime.BudgetLabels
	for _, provider := range providersync.MatrixProviders() {
		for _, costClass := range costClasses {
			labels = append(labels, jobruntime.BudgetLabels{
				Provider: provider, CostClass: string(costClass),
			})
		}
	}
	return labels
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

// profileReady is the production call site for exact startup validation. It
// proves the registry's executable coverage, the constructed queue consumers,
// and the deployment budget in one place, so no other readiness path can
// approve a partially constructed profile.
func (dependencies *workerDependencies) profileReady(context.Context) error {
	if dependencies == nil || dependencies.registryErr != nil ||
		dependencies.runtimeRegistry == nil || dependencies.startupErr != nil {
		return errWorkerDependencyUnavailable
	}
	if err := dependencies.runtimeRegistry.ValidateStartup(dependencies.startup); err != nil {
		return errWorkerDependencyUnavailable
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
