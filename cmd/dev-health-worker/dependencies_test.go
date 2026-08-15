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

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
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
	if workerSpec.Service != "dev-health-worker" || workerSpec.DefaultProfile != "" {
		t.Fatalf("unexpected worker spec: %#v", workerSpec)
	}
	if !slices.Equal(workerSpec.Profiles, []string{"sync", "heavy", "ops"}) {
		t.Fatalf("unexpected worker profiles: %v", workerSpec.Profiles)
	}
	if workerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal("worker dependency configuration is not wired")
	}
}

func TestRiverClientsShareOneProcessIdentityWithDistinctFamilySuffixes(t *testing.T) {
	t.Parallel()
	cfg := config.Config{WorkerInstanceID: "11111111-1111-4111-8111-111111111111"}
	if got := riverClientID(cfg, "operational"); got != cfg.WorkerInstanceID+"-operational" {
		t.Fatalf("operational client ID = %q", got)
	}
	if got := riverClientID(cfg, "daily"); got != cfg.WorkerInstanceID+"-daily" {
		t.Fatalf("daily client ID = %q", got)
	}
	if got := riverClientID(config.Config{}, "daily"); got != "" {
		t.Fatalf("test client ID = %q, want River default", got)
	}
}

func TestNoDatabaseConfigurationStaysLiveAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependencies(
		context.Background(),
		config.Config{Profile: "ops", RiverDatabaseSchema: "river"},
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
	// Every accepted profile now owns registered kinds, so queue telemetry is
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
		"profile_completeness",
		"queue_postgres",
		"queued_contract_versions",
		"river_schema",
	}
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestProviderRouteSwitchesAreIndependentAndRejectIncompleteRoutes(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	workItems := validGitHubWorkItemsRuntimeConfig(t)
	gitlabWorkItems := workItems
	gitlabWorkItems.WorkerGithubWorkItemsEnabled = false
	gitlabWorkItems.WorkerGitlabWorkItemsEnabled = true
	jiraWorkItems := workItems
	jiraWorkItems.WorkerGithubWorkItemsEnabled = false
	jiraWorkItems.WorkerJiraWorkItemsEnabled = true
	linearWorkItems := workItems
	linearWorkItems.WorkerGithubWorkItemsEnabled = false
	linearWorkItems.WorkerLinearWorkItemsEnabled = true
	for _, test := range []struct {
		name    string
		config  config.Config
		runtime bool
		wantErr bool
	}{
		{name: "all off", config: config.Config{}},
		{
			name: "launchdarkly complete",
			config: config.Config{
				WorkerLaunchDarklyFeatureFlagsEnabled: true,
			},
			runtime: true,
		},
		{
			name: "launchdarkly missing runtime",
			config: config.Config{
				WorkerLaunchDarklyFeatureFlagsEnabled: true,
			},
			wantErr: true,
		},
		{
			// (github, repo-metadata) is the second RouteReady pair added by
			// CHAOS-3123; this case pins that the generalised runtimeConstructed
			// check (any enabled route, not launchdarkly alone) actually covers
			// it rather than only having been exercised for launchdarkly.
			name: "github complete",
			config: config.Config{
				WorkerGithubRepoMetadataEnabled: true,
			},
			runtime: true,
		},
		{
			name: "github missing runtime",
			config: config.Config{
				WorkerGithubRepoMetadataEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "gitlab complete",
			config: config.Config{
				WorkerGitlabRepoMetadataEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab missing runtime",
			config: config.Config{
				WorkerGitlabRepoMetadataEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "gitlab commits complete",
			config: config.Config{
				WorkerGitlabCommitsEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab commits missing runtime",
			config: config.Config{
				WorkerGitlabCommitsEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "gitlab commit stats complete",
			config: config.Config{
				WorkerGitlabCommitStatsEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab commit stats missing runtime",
			config: config.Config{
				WorkerGitlabCommitStatsEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "gitlab cicd complete",
			config: config.Config{
				WorkerGitlabCICDEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab tests complete",
			config: config.Config{
				WorkerGitlabTestsEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab incidents complete",
			config: config.Config{
				WorkerGitlabIncidentsEnabled: true,
			},
			runtime: true,
		},
		{
			name: "gitlab incidents missing runtime",
			config: config.Config{
				WorkerGitlabIncidentsEnabled: true,
			},
			wantErr: true,
		},
		{name: "github prs complete", config: config.Config{WorkerGithubPRsEnabled: true}, runtime: true},
		{name: "github prs missing runtime", config: config.Config{WorkerGithubPRsEnabled: true}, wantErr: true},
		{name: "github pr reviews complete", config: config.Config{WorkerGithubPRReviewsEnabled: true}, runtime: true},
		{name: "github pr reviews missing runtime", config: config.Config{WorkerGithubPRReviewsEnabled: true}, wantErr: true},
		{name: "github pr comments complete", config: config.Config{WorkerGithubPRCommentsEnabled: true}, runtime: true},
		{name: "github pr comments missing runtime", config: config.Config{WorkerGithubPRCommentsEnabled: true}, wantErr: true},
		{name: "gitlab deployments complete", config: config.Config{WorkerGitlabDeploymentsEnabled: true}, runtime: true},
		{name: "gitlab feature flags complete", config: config.Config{WorkerGitlabFeatureFlagsEnabled: true}, runtime: true},
		{name: "gitlab files complete", config: config.Config{WorkerGitlabFilesEnabled: true}, runtime: true},
		{name: "gitlab blame complete", config: config.Config{WorkerGitlabBlameEnabled: true}, runtime: true},
		{name: "gitlab prs complete", config: config.Config{WorkerGitlabPRsEnabled: true}, runtime: true},
		{name: "gitlab pr reviews complete", config: config.Config{WorkerGitlabPRReviewsEnabled: true}, runtime: true},
		{name: "gitlab pr comments complete", config: config.Config{WorkerGitlabPRCommentsEnabled: true}, runtime: true},
		{name: "gitlab security complete", config: config.Config{WorkerGitlabSecurityEnabled: true}, runtime: true},
		{name: "gitlab work items complete", config: gitlabWorkItems, runtime: true},
		{name: "pagerduty services complete", config: config.Config{WorkerPagerDutyServicesEnabled: true}, runtime: true},
		{name: "pagerduty business services complete", config: config.Config{WorkerPagerDutyBusinessServicesEnabled: true}, runtime: true},
		{name: "pagerduty escalation policies complete", config: config.Config{WorkerPagerDutyEscalationPoliciesEnabled: true}, runtime: true},
		{name: "pagerduty schedules complete", config: config.Config{WorkerPagerDutySchedulesEnabled: true}, runtime: true},
		{name: "pagerduty on calls complete", config: config.Config{WorkerPagerDutyOnCallsEnabled: true}, runtime: true},
		{name: "pagerduty users complete", config: config.Config{WorkerPagerDutyUsersEnabled: true}, runtime: true},
		{name: "pagerduty teams complete", config: config.Config{WorkerPagerDutyTeamsEnabled: true}, runtime: true},
		{name: "pagerduty incident family complete", config: config.Config{WorkerPagerDutyIncidentsEnabled: true}, runtime: true},
		{
			name:    "linear work items complete",
			config:  linearWorkItems,
			runtime: true,
		},
		{
			name:    "jira work items complete",
			config:  jiraWorkItems,
			runtime: true,
		},
		{
			name:    "jira incidents incomplete",
			config:  config.Config{WorkerJiraIncidentsEnabled: true},
			wantErr: true,
		},
	} {
		err := providerRouteSwitchesReady(
			test.config, &test.runtime,
		)(context.Background())
		if (err != nil) != test.wantErr {
			t.Fatalf("%s error=%v wantErr=%v", test.name, err, test.wantErr)
		}
	}
}

func TestLocalAllStartupReadinessProjectsEveryCompleteWriterAlias(t *testing.T) {
	values := map[string]string{
		"DEV_HEALTH_ENV":     "local",
		"GO_PROVIDER_ROUTES": "all",
		"DEV_HEALTH_PROFILE": "sync",
	}
	cfg, err := config.Load(config.Spec{
		Service: "dev-health-worker", Profiles: []string{"sync"}, DefaultProfile: "sync",
		LookupEnv: func(key string) (string, bool) {
			value, ok := values[key]
			return value, ok
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	effective := map[string]bool{}
	for _, route := range effectiveProviderRouteSwitches(cfg) {
		effective[route.provider+"/"+route.dataset] = true
	}
	for _, identity := range []string{
		"github/pr-reviews", "github/pr-comments", "github/tests",
		"gitlab/pr-reviews", "gitlab/pr-comments", "gitlab/tests",
	} {
		if !effective[identity] {
			t.Fatalf("startup readiness omitted inherited route %s", identity)
		}
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
				WorkerGithubWorkItemsEnabled:              true,
				WorkerGithubWorkItemsInvestmentConfigPath: valid.WorkerGithubWorkItemsInvestmentConfigPath,
			},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := providerRouteSwitchesReady(test.config, &test.runtime)(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}

	t.Setenv("STATUS_MAPPING_PATH", " ")
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
	// too, so the sync profile is only complete once something reports it.
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
	sources.buildOperational = nil
	sources.buildSyncCoordinator = fakeHandlerBuilder(
		"sync-coordinator",
		[]jobruntime.HandlerSpec{autoimportSpec},
		jobruntime.QueueBudget{Queue: syncCoordinatorQueue, MaxWorkers: syncCoordinatorQueueWorkers},
	)
	sources.buildProviderSync = func(
		_ context.Context,
		_ config.Config,
		_ workerDatabase,
		_ *jobruntime.Registry,
		_ jobruntime.Observer,
		_ *slog.Logger,
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
		config.Config{
			Profile: "sync", RiverDatabaseSchema: "river",
			DomainDatabaseMaxConns:                4,
			QueueDatabaseMaxConns:                 2,
			WorkerLaunchDarklyFeatureFlagsEnabled: true,
		},
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
	// LaunchDarkly provider-route-switch gate itself, and partial handler
	// coverage (team_autoimport alone) would instead fail closed at
	// construction time -- the invariant TestNestedRiverClientCapabilityIsValidated
	// already proves -- before the readiness gate this assertion inspects
	// ever opens.
	sources.buildSyncCoordinator = nil
	missingRegistry := health.NewRegistry(100 * time.Millisecond)
	_, err = configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Profile: "sync", RiverDatabaseSchema: "river",
			DomainDatabaseMaxConns:                4,
			QueueDatabaseMaxConns:                 2,
			WorkerLaunchDarklyFeatureFlagsEnabled: true,
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
		!slices.Contains(missing.Failed, "profile_completeness") {
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
	// The ops kinds now ship at go_default, so the production operational
	// builder would demand a real postgres pool this test's fake database
	// cannot satisfy. This test only exercises the metrics path (registry-
	// bounded dimensions over queue telemetry), so the ops profile is
	// demoted back to Celery here to keep it dormant and out of the way.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOpsKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
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

// celeryRoutedOpsKinds is every ops-profile kind. The checked-in contract now
// ships all five at go_default, so a genuinely dormant ops profile has to be
// built explicitly by demoting them back to Celery in a scoped fixture.
var celeryRoutedOpsKinds = []string{
	jobcontract.KindBillingNotification,
	jobcontract.KindWebhookDelivery,
	jobcontract.KindHeartbeat,
	jobcontract.KindRetentionCleanup,
	jobcontract.KindSyncCoverageRefresh,
}

func TestCeleryRoutedHandlersCannotPassProfileCompleteness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeWorkerDatabase{domainSaturation: 0.25, queueSaturation: 0.5}
	_, contractRoot := demotedContractRoot(t, celeryRoutedOpsKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Profile:                "ops",
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
	if status.Ready || !slices.Equal(status.Failed, []string{"profile_completeness"}) {
		t.Fatalf("readiness = %#v, want only profile_completeness failure", status)
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
	reportKinds := map[string]bool{
		jobcontract.KindReportExecuteOnDemand:  true,
		jobcontract.KindReportExecuteScheduled: true,
	}
	// dailyKinds also covers the remaining-metrics families: their route is
	// cross-validated against families.json's unconditional "river" value,
	// so they stay executable regardless of this fixture and the fake
	// "daily" builder below must report them or profile completeness sees
	// uncovered registry kinds.
	dailyKinds := map[string]bool{
		jobcontract.KindDailyMetricsDispatch:     true,
		jobcontract.KindDailyMetricsPartition:    true,
		jobcontract.KindDailyMetricsFinalize:     true,
		jobcontract.KindRemainingCapacity:        true,
		jobcontract.KindRemainingComplexity:      true,
		jobcontract.KindRemainingDORA:            true,
		jobcontract.KindRemainingExtraMetrics:    true,
		jobcontract.KindRemainingMembership:      true,
		jobcontract.KindRemainingRecommendations: true,
		jobcontract.KindRemainingReleaseImpact:   true,
		jobcontract.KindRemainingTeamMetrics:     true,
	}
	// The real report builder is replaced by a fake here so the composition
	// rules are tested without a ClickHouse dependency; the production builder
	// has its own coverage.
	sources.buildReports = nil
	sources.buildOperational = fakeHandlerBuilder(
		"reports", selectSpecs(runtimeRegistry.Profile("heavy"), reportKinds),
		jobruntime.QueueBudget{Queue: "reports", MaxWorkers: 2},
	)
	sources.buildDaily = fakeHandlerBuilder(
		"daily", selectSpecs(runtimeRegistry.Profile("heavy"), dailyKinds),
		jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
	)

	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Profile:                "heavy",
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
		components[3].Name() != "river-profile-workers" {
		t.Fatalf("composed components = %#v", components)
	}
	profileWorkers, ok := components[3].(workerProfileComponent)
	if !ok || len(profileWorkers.components) != 2 ||
		profileWorkers.components[0].Name() != "reports" ||
		profileWorkers.components[1].Name() != "daily" ||
		profileWorkers.ShutdownBudget() != 7_200*time.Second {
		t.Fatalf("profile workers = %#v", components[3])
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
			Profile:                  "heavy",
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

func TestProductionOpsBuilderConstructsNativeSyncCoverageRefresh(t *testing.T) {
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
			Profile:                  "ops",
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
	if len(components) != 6 || components[0].Name() != "postgres-runtime-pools" ||
		components[2].Name() != "preclaim-readiness" ||
		components[3].Name() != "worker-profile-presence" ||
		components[4].Name() != "river-profile-workers" ||
		components[5].Name() != "worker-profile-drain" {
		t.Fatalf("production components = %#v", components)
	}
	if err := components[0].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerProfileRejectsProcessShutdownTimeoutBelowContract(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	profile := "ops"
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return &fakeWorkerDatabase{}, nil
	}
	sources.loadDeployment = func(
		string, jobcontract.Registry,
	) (deploymentcontract.Manifest, deploymentcontract.BudgetSummary, error) {
		return deploymentcontract.Manifest{Processes: []deploymentcontract.Process{{
			Name: "ops", Runtime: "river", RegistryProfile: &profile,
			ShutdownGraceSeconds: 960,
		}}}, deploymentcontract.BudgetSummary{}, nil
	}
	dependencies := buildWorkerDependencies(context.Background(), config.Config{
		Profile: "ops", ShutdownTimeout: 959 * time.Second,
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
	// Demote the ops kinds so the production operational builder (which now
	// requires a real postgres pool once they are executable) stays a
	// no-op; this test only exercises the queued-contract-versions gate.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOpsKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
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
	// Demote the ops kinds so the production operational builder (which now
	// requires a real postgres pool once they are executable) stays a
	// no-op; this test only exercises the queue-telemetry metrics path.
	_, contractRoot := demotedContractRoot(t, celeryRoutedOpsKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
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

func TestWorkerProfileStartRollbackUsesReviewedShutdownBudget(t *testing.T) {
	t.Parallel()
	started := &deadlineShutdownComponent{deadline: make(chan time.Time, 1)}
	budget := time.Second
	component := workerProfileComponent{
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

func TestWorkerProfileComponentStopsIndependentRiverClientsConcurrently(t *testing.T) {
	t.Parallel()
	first := &blockingShutdownComponent{
		name: "first", entered: make(chan struct{}), release: make(chan struct{}),
	}
	second := &blockingShutdownComponent{
		name: "second", entered: make(chan struct{}), release: make(chan struct{}),
	}
	component := workerProfileComponent{
		components: []lifecycle.Component{first, second}, budget: time.Minute,
	}
	done := make(chan error, 1)
	go func() { done <- component.Shutdown(context.Background()) }()
	for _, entered := range []<-chan struct{}{first.entered, second.entered} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("River clients did not enter shutdown concurrently")
		}
	}
	close(first.release)
	close(second.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func fakeHandlerBuilder(
	name string,
	specs []jobruntime.HandlerSpec,
	queues ...jobruntime.QueueBudget,
) workerFamilyBuilder {
	return func(
		config.Config,
		workerDatabase,
		*jobruntime.Registry,
		jobruntime.Observer,
		*slog.Logger,
	) (workerFamily, error) {
		return workerFamily{
			component: namedComponent(name), handlers: specs, queues: queues,
		}, nil
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
	// The checked-in contract now ships every heavy-profile kind at
	// go_default, but this fixture only exercises the daily-metrics and
	// (optionally) report builders. work-graph and investment have no
	// builder in this binary at all, so they are demoted back to Celery here
	// -- otherwise the heavy profile would demand handlers nothing ever
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
	snapshot := riverstore.QueueTelemetrySnapshot{Profile: config.Profile}
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
			Profile:                  "heavy",
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

// TestFakeBuilderCannotSatisfyProfileCoverage proves a builder cannot buy
// readiness by inventing handler specs. Coverage counts alone are not enough:
// every constructed spec is compared field by field with the registry.
func TestFakeBuilderCannotSatisfyProfileCoverage(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	runtimeRegistry, contractRoot := executableHeavyRegistry(t, true)
	heavy := runtimeRegistry.Profile("heavy")
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
					Profile:                "heavy",
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
	// report them too or profile completeness sees uncovered registry kinds
	// before the queue-budget assertions this test actually exercises ever
	// run.
	daily := selectSpecs(runtimeRegistry.Profile("heavy"), map[string]bool{
		jobcontract.KindDailyMetricsDispatch:     true,
		jobcontract.KindDailyMetricsPartition:    true,
		jobcontract.KindDailyMetricsFinalize:     true,
		jobcontract.KindRemainingCapacity:        true,
		jobcontract.KindRemainingComplexity:      true,
		jobcontract.KindRemainingDORA:            true,
		jobcontract.KindRemainingExtraMetrics:    true,
		jobcontract.KindRemainingMembership:      true,
		jobcontract.KindRemainingRecommendations: true,
		jobcontract.KindRemainingReleaseImpact:   true,
		jobcontract.KindRemainingTeamMetrics:     true,
	})
	for _, test := range []struct {
		name    string
		queue   jobruntime.QueueBudget
		conns   jobruntime.ConnectionBudget
		wantErr bool
	}{
		{
			name:  "matching budget",
			queue: jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
			conns: jobruntime.ConnectionBudget{QueueControl: 2, Domain: 4},
		},
		{
			name:    "over-budget workers",
			queue:   jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 8},
			conns:   jobruntime.ConnectionBudget{QueueControl: 2, Domain: 4},
			wantErr: true,
		},
		{
			name:    "unbudgeted queue",
			queue:   jobruntime.QueueBudget{Queue: "metrics_shadow", MaxWorkers: 2},
			conns:   jobruntime.ConnectionBudget{QueueControl: 2, Domain: 4},
			wantErr: true,
		},
		{
			name:    "over-budget connections",
			queue:   jobruntime.QueueBudget{Queue: "metrics", MaxWorkers: 2},
			conns:   jobruntime.ConnectionBudget{QueueControl: 4, Domain: 8},
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
			sources.buildReports = nil
			sources.buildWorkgraph = nil
			sources.buildOperational = nil
			sources.buildDaily = fakeHandlerBuilder("daily", daily, test.queue)
			_, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{
					Profile:                "heavy",
					RiverDatabaseSchema:    "river",
					DomainDatabaseMaxConns: int32(test.conns.Domain),
					QueueDatabaseMaxConns:  int32(test.conns.QueueControl),
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

// TestNestedRiverClientCapabilityIsValidated proves a handler hosted in a
// builder's own private River client is covered by exact startup validation.
// sync.team_autoimport lives in the sync coordinator's client, not the provider
// client, so before CUT-02 it could be constructed and consumed while readiness
// had no way to observe it. Capability now reaches validation through one
// canonical channel regardless of which client hosts the worker.
func TestNestedRiverClientCapabilityIsValidated(t *testing.T) {
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
		Profile:                "sync",
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
		sources.buildProviderSync = func(
			context.Context, config.Config, workerDatabase,
			*jobruntime.Registry, jobruntime.Observer, *slog.Logger,
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
		{name: "reported nested handler completes the profile", reported: true, wantReady: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := &fakeWorkerDatabase{}
			sources := baseSources()
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return database, nil
			}
			sources.buildSyncCoordinator = func(
				config.Config, workerDatabase, *jobruntime.Registry,
				jobruntime.Observer, *slog.Logger,
			) (workerFamily, error) {
				family := workerFamily{component: namedComponent("sync-coordinator")}
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
			if failed := slices.Contains(status.Failed, "profile_completeness"); failed == test.wantReady {
				t.Fatalf("readiness = %#v, want profile_completeness failure = %t",
					status, !test.wantReady)
			}
		})
	}
}
