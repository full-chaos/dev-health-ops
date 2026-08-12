package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type providerSyncEntitlementFunc func(context.Context, string) error

func (require providerSyncEntitlementFunc) Require(ctx context.Context, orgID string) error {
	return require(ctx, orgID)
}

// githubWorkItemsBuildExecutorConn is deliberately only a non-nil driver.Conn
// marker. Both GitHub work-item constructors store their connection during
// BuildExecutor construction and perform I/O only when the handler executes,
// which lets this test reach the production config propagation seam without a
// ClickHouse server.
type githubWorkItemsBuildExecutorConn struct{ driver.Conn }

func TestBuildProviderSyncHandlerConstructsAggregateGitLabRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dataset     string
		handlerType string
		sinkType    string
	}{
		{"deployments", "providersync.GitLabDeploymentsRouteHandler", "providersync.GitLabDeploymentsClickHouseEffects"},
		{"feature-flags", "providersync.GitLabFeatureFlagsRouteHandler", "providersync.GitLabFeatureFlagsClickHouseEffects"},
		{"files", "providersync.GitLabFilesRouteHandler", "providersync.GitLabFilesClickHouseEffects"},
		{"blame", "providersync.GitLabBlameRouteHandler", "providersync.GitLabBlameClickHouseEffects"},
		{"prs", "providersync.GitLabPullRequestRouteHandler", "providersync.GitLabPullRequestSocialClickHouseEffects"},
		{"pr-reviews", "providersync.GitLabPullRequestRouteHandler", "providersync.GitLabPullRequestSocialClickHouseEffects"},
		{"pr-comments", "providersync.GitLabPullRequestRouteHandler", "providersync.GitLabPullRequestSocialClickHouseEffects"},
		{"security", "providersync.GitLabSecurityRouteHandler", "providersync.GitLabSecurityClickHouseEffects"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.dataset, func(t *testing.T) {
			t.Parallel()
			handler, _ := buildProviderSyncHandler(
				nil, providersync.CompleteRouteSwitches{}, nil, nil, nil, nil,
				nil, nil, slog.Default(),
			)
			executor, err := handler.BuildExecutor(&providersync.LeaseSession{
				Claim: providersync.Claim{Unit: providersync.Unit{
					Provider: "gitlab", Dataset: test.dataset,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%T", executor.Handler); got != test.handlerType {
				t.Fatalf("handler=%s want=%s", got, test.handlerType)
			}
			if got := fmt.Sprintf("%T", executor.Committer.Sink); got != test.sinkType {
				t.Fatalf("sink=%s want=%s", got, test.sinkType)
			}
			if got := fmt.Sprintf("%T", executor.Committer.Readback); got != test.sinkType {
				t.Fatalf("readback=%s want=%s", got, test.sinkType)
			}
		})
	}
}

func TestBuildProviderSyncHandlerConstructsAggregateWorkItemRoutes(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(validGitHubWorkItemsRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
		nil, providersync.CompleteRouteSwitches{}, nil,
		&githubWorkItemsBuildExecutorConn{}, nil, nil, nil, nil,
		slog.Default(), runtimeConfig,
	)
	for _, test := range []struct {
		provider    string
		handlerType string
		sinkType    string
	}{
		{"gitlab", "providersync.GitLabWorkItemsRouteHandler", "providersync.GitLabWorkItemFamilyClickHouseEffects"},
		{"jira", "providersync.JiraAtlassianRouteHandler", "providersync.JiraWorkItemCompositeClickHouseEffects"},
		{"linear", "providersync.LinearWorkItemFamilyRouteHandler", "providersync.LinearWorkItemFamilyClickHouseEffects"},
	} {
		t.Run(test.provider, func(t *testing.T) {
			executor, err := handler.BuildExecutor(&providersync.LeaseSession{
				Claim: providersync.Claim{Unit: providersync.Unit{
					Provider: test.provider, Dataset: "work-items",
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%T", executor.Handler); got != test.handlerType {
				t.Fatalf("handler=%s want=%s", got, test.handlerType)
			}
			if got := fmt.Sprintf("%T", executor.Committer.Sink); got != test.sinkType {
				t.Fatalf("sink=%s want=%s", got, test.sinkType)
			}
		})
	}
}

func TestBuildProviderSyncHandlerConfiguresLinearAccountDiscovery(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(validGitHubWorkItemsRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
		nil, providersync.CompleteRouteSwitches{}, nil,
		&githubWorkItemsBuildExecutorConn{}, nil, nil, nil, nil,
		slog.Default(), runtimeConfig,
	)
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "linear", Dataset: "work-items",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	linearHandler, ok := executor.Handler.(providersync.LinearWorkItemFamilyRouteHandler)
	if !ok {
		t.Fatalf("handler=%T want providersync.LinearWorkItemFamilyRouteHandler", executor.Handler)
	}
	if !linearHandler.Direct.GlobalDiscovery {
		t.Fatal("production Linear account unit must discover every accessible team")
	}
}

func TestBuildProviderSyncHandlerConstructsPagerDutyRoutesWithCredentialBoundEffects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dataset     string
		handlerType string
		sinkType    string
	}{
		{"services", "providersync.PagerDutyServicesRouteHandler", "providersync.PagerDutyServicesClickHouseEffects"},
		{"business-services", "providersync.PagerDutyBusinessServicesRouteHandler", "providersync.PagerDutyBusinessServicesClickHouseEffects"},
		{"escalation-policies", "providersync.PagerDutyEscalationPoliciesRouteHandler", "providersync.PagerDutyEscalationPoliciesClickHouseEffects"},
		{"schedules", "providersync.PagerDutySchedulesRouteHandler", "providersync.PagerDutySchedulesClickHouseEffects"},
		{"on-calls", "providersync.PagerDutyOnCallsRouteHandler", "providersync.PagerDutyOnCallsClickHouseEffects"},
		{"users", "providersync.PagerDutyUsersRouteHandler", "providersync.PagerDutyUsersClickHouseEffects"},
		{"teams", "providersync.PagerDutyTeamsRouteHandler", "providersync.PagerDutyTeamsClickHouseEffects"},
		{"incidents", "providersync.PagerDutyIncidentFamilyRouteHandler", "providersync.PagerDutyIncidentFamilyClickHouseEffects"},
		{"incident-alerts", "providersync.PagerDutyIncidentFamilyRouteHandler", "providersync.PagerDutyIncidentFamilyClickHouseEffects"},
		{"incident-log-entries", "providersync.PagerDutyIncidentFamilyRouteHandler", "providersync.PagerDutyIncidentFamilyClickHouseEffects"},
		{"incident-notes", "providersync.PagerDutyIncidentFamilyRouteHandler", "providersync.PagerDutyIncidentFamilyClickHouseEffects"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.dataset, func(t *testing.T) {
			t.Parallel()
			handler, _ := buildProviderSyncHandler(
				nil, providersync.CompleteRouteSwitches{}, nil, nil, nil, nil,
				nil, nil, slog.Default(),
			)
			executor, err := handler.BuildExecutor(&providersync.LeaseSession{
				Claim: providersync.Claim{Unit: providersync.Unit{
					Provider: "pagerduty", Dataset: test.dataset,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%T", executor.Handler); got != test.handlerType {
				t.Fatalf("handler=%s want=%s", got, test.handlerType)
			}
			if executor.EffectsFactory == nil || executor.Committer.Sink != nil {
				t.Fatalf("effects factory=%v startup sink=%T", executor.EffectsFactory != nil, executor.Committer.Sink)
			}
			sink, readback, err := executor.EffectsFactory(providerfoundation.Credential{
				Provider: "pagerduty", Config: map[string]string{"subdomain": " Acme "},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%T", sink); got != test.sinkType {
				t.Fatalf("sink=%s want=%s", got, test.sinkType)
			}
			if got := fmt.Sprintf("%T", readback); got != test.sinkType {
				t.Fatalf("readback=%s want=%s", got, test.sinkType)
			}
			value := reflect.ValueOf(sink)
			if field := value.FieldByName("ProviderInstanceID"); field.IsValid() && field.String() != "acme" {
				t.Fatalf("provider instance=%q want=acme", field.String())
			}
		})
	}
}

// TestBuildProviderSyncHandlerSharesOneMetricsInstance is CHAOS-3118
// mutation-tested evidence for dev_health_provider_*, not a behavioral
// assertion: it pins the identity between the providerfoundation.Metrics
// instance BuildExecutor hands each claim's executor and the instance the
// caller (buildProviderSyncWorker) registers as workerFamily.metricsSource.
//
// The original defect on origin/main was exactly this: providerfoundation.NewMetrics()
// was called inside the BuildExecutor closure itself, so every unit dispatch
// got a fresh, immediately-discarded instance while a *different* instance —
// or none at all — was what got registered and scraped. Every counter reset
// to zero on the next dispatch and the family was permanently invisible.
//
// A behavioral test (record something, read it back) cannot catch that
// defect: reverting just "Metrics: providerMetrics" to
// "Metrics: providerfoundation.NewMetrics()" inside BuildExecutor, while
// leaving workerFamily.metricsSource registration untouched, still compiles,
// still passes every other test in this package and in
// internal/providerfoundation, and still publishes HELP/TYPE — it looks
// exactly as wired as the real fix. Only a same-instance check distinguishes
// them, which is why this test compares pointers rather than values.
func TestBuildProviderSyncHandlerSharesOneMetricsInstance(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{
		Profiles: []string{"sync"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Every dependency below is only ever stored as a closure capture inside
	// buildProviderSyncHandler — never dialed, dereferenced, or called during
	// construction — so nil stand-ins are sufficient to reach the real,
	// unmodified BuildExecutor closure without a live ClickHouse, Valkey, or
	// Postgres connection.
	handler, providerMetrics := buildProviderSyncHandler(
		nil, providersync.CompleteRouteSwitches{}, nil, nil, nil, nil,
		nil, collector, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("buildProviderSyncHandler returned an incomplete handler")
	}
	if providerMetrics == nil {
		t.Fatal("buildProviderSyncHandler returned a nil Metrics instance")
	}

	// BuildExecutor selects its CompleteRouteHandler/effect sink from
	// session.Claim (CHAOS-3123: no more single hardcoded route), so a
	// zero-value Claim no longer reaches the closure this test targets — it
	// hits the closure's own fail-closed default case instead. Any one of the
	// route-ready pairs proves the same metrics-identity seam; launchdarkly is
	// arbitrary here.
	session := &providersync.LeaseSession{
		Claim: providersync.Claim{
			Unit: providersync.Unit{
				Provider: "launchdarkly", Dataset: "feature-flags",
			},
		},
	}
	executor, err := handler.BuildExecutor(session)
	if err != nil {
		t.Fatalf("BuildExecutor: %v", err)
	}

	// The property under test is identity, not behavior: this is the same
	// check buildProviderSyncWorker's real wiring must satisfy between
	// executor.Metrics (what every claim actually writes to) and
	// workerFamily.metricsSource (what the health.Registry actually scrapes).
	if executor.Metrics != providerMetrics {
		t.Fatalf(
			"executor.Metrics = %p, want the same instance buildProviderSyncHandler "+
				"returned (%p): a distinct instance here writes real counters nobody "+
				"scrapes, while the registered instance stays permanently zero — the "+
				"exact CHAOS-3118 defect",
			executor.Metrics, providerMetrics,
		)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubWorkItemsWithValidatedRuntimeConfig(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(validGitHubWorkItemsRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
		nil,
		providersync.CompleteRouteSwitches{GithubWorkItems: true},
		nil,
		&githubWorkItemsBuildExecutorConn{},
		nil,
		nil,
		nil,
		nil,
		slog.Default(),
		runtimeConfig,
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "work-items",
		}},
	})
	if err != nil {
		t.Fatalf("BuildExecutor with validated paths: %v", err)
	}
	if _, ok := executor.Handler.(providersync.GitHubWorkItemsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubWorkItemClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}

	withoutPaths, _ := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
		nil,
		providersync.CompleteRouteSwitches{GithubWorkItems: true},
		nil,
		&githubWorkItemsBuildExecutorConn{},
		nil,
		nil,
		nil,
		nil,
		slog.Default(),
		githubWorkItemsRuntimeConfig{},
	)
	_, err = withoutPaths.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "work-items",
		}},
	})
	if !errors.Is(err, providersync.ErrInvalidConfiguration) {
		t.Fatalf("BuildExecutor without explicit paths error=%v want ErrInvalidConfiguration", err)
	}
}

type independentRiverClient struct {
	name     string
	config   *river.Config
	handlers map[string]struct{}
}

func TestIndependentSyncClientsDoNotShareQueuesWithDisjointHandlers(
	t *testing.T,
) {
	coordinator := independentRiverClient{
		name: "sync-coordinator",
		config: &river.Config{
			Queues: map[string]river.QueueConfig{
				"sync": {MaxWorkers: 2},
			},
		},
		handlers: handlerSet(
			syncdispatchcontract.KindDispatchSyncRun,
			syncdispatchcontract.KindFinalizeSyncRun,
			syncdispatchcontract.KindPostSync,
			syncdispatchcontract.KindReferenceDiscovery,
		),
	}
	provider := independentRiverClient{
		name: "provider-unit",
		config: providerSyncRiverConfig(
			slog.Default(), river.NewWorkers(), "river",
		),
		handlers: handlerSet(jobcontract.KindSyncProviderUnit),
	}

	assertNoSharedQueueWithDisjointHandlers(t, coordinator, provider)
	if _, ok := provider.config.Queues[providerUnitQueue]; !ok {
		t.Fatalf("provider client does not own %q", providerUnitQueue)
	}
}

func TestProviderSyncClientOwnsItsRegistryQueue(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok {
		t.Fatal("provider-unit descriptor missing")
	}
	config := providerSyncRiverConfig(
		slog.Default(), river.NewWorkers(), "river",
	)
	if descriptor.Queue != providerUnitQueue {
		t.Fatalf(
			"provider-unit registry queue=%q want=%q",
			descriptor.Queue, providerUnitQueue,
		)
	}
	if _, ok := config.Queues[descriptor.Queue]; !ok {
		t.Fatalf("provider client does not consume registry queue %q", descriptor.Queue)
	}
}

func TestProviderSyncRiverConfigPassesRiverClientValidation(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgresql://unused:unused@127.0.0.1:1/unused",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, err = river.NewClient(
		riverpgxv5.New(pool),
		providerSyncRiverConfig(
			slog.Default(), river.NewWorkers(), "river",
		),
	)
	if err != nil {
		t.Fatalf("provider sync River config is invalid: %v", err)
	}
}

func handlerSet(kinds ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		result[kind] = struct{}{}
	}
	return result
}

func assertNoSharedQueueWithDisjointHandlers(
	t *testing.T,
	clients ...independentRiverClient,
) {
	t.Helper()
	for left := range clients {
		for right := left + 1; right < len(clients); right++ {
			if !disjointHandlerSets(clients[left].handlers, clients[right].handlers) {
				continue
			}
			for queue := range clients[left].config.Queues {
				if _, shared := clients[right].config.Queues[queue]; shared {
					t.Fatalf(
						"independent River clients %q and %q share queue %q with disjoint handlers",
						clients[left].name, clients[right].name, queue,
					)
				}
			}
		}
	}
}

func disjointHandlerSets(left, right map[string]struct{}) bool {
	for kind := range left {
		if _, shared := right[kind]; shared {
			return false
		}
	}
	return true
}

// TestProviderSyncHandlerSwitchesFollowConfiguration pins the CHAOS-3123 fix
// that the executing handler reads the same switches the readiness check does.
//
// The defect this replaces was a literal `LaunchDarklyFeatureFlags: true`
// inside buildProviderSyncHandler. That is invisible to every behavioral test:
// the handler serves LaunchDarkly whether or not the process was configured to,
// and refuses (github, repo-metadata) even when it was — so a unit the Python
// producer legitimately routed to River is answered with a route fault rather
// than executed. Reintroducing any hardcoded field here must fail this test.
func TestProviderSyncHandlerSwitchesFollowConfiguration(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]providersync.CompleteRouteSwitches{
		"none":               {},
		"github":             {GithubRepoMetadata: true},
		"gitlab":             {GitlabRepoMetadata: true},
		"gitlab_commits":     {GitlabCommits: true},
		"gitlab_stats":       {GitlabCommitStats: true},
		"gitlab_cicd":        {GitlabCICD: true},
		"gitlab_tests":       {GitlabTests: true},
		"gitlab_incidents":   {GitlabIncidents: true},
		"github_prs":         {GithubPRs: true},
		"github_pr_reviews":  {GithubPRReviews: true},
		"github_pr_comments": {GithubPRComments: true},
		"github_cicd":        {GithubCICD: true},
		"github_security":    {GithubSecurity: true},
		"both":               {GithubRepoMetadata: true, LaunchDarklyFeatureFlags: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			handler, _ := buildProviderSyncHandler(
				nil, want, nil, nil, nil, nil, nil, nil, slog.Default(),
			)
			if handler == nil {
				t.Fatal("buildProviderSyncHandler returned no handler")
			}
			if handler.Switches != want {
				t.Fatalf("handler.Switches = %+v, want %+v", handler.Switches, want)
			}
		})
	}
}

func TestBuildProviderSyncWorkerConstructsForEveryRouteReadySwitch(t *testing.T) {
	originalConstructor := constructProviderSyncWorker
	t.Cleanup(func() {
		constructProviderSyncWorker = originalConstructor
	})

	constructProviderSyncWorker = func(
		_ context.Context,
		_ config.Config,
		_ workerDatabase,
		_ *jobruntime.Registry,
		_ jobruntime.Observer,
		_ *slog.Logger,
	) (workerFamily, error) {
		return workerFamily{
			queues: []jobruntime.QueueBudget{{
				Queue: providerUnitQueue, MaxWorkers: providerUnitQueueWorkers,
			}},
		}, nil
	}

	for _, test := range []struct {
		name string
		cfg  config.Config
	}{
		{
			name: "launchdarkly feature flags",
			cfg: config.Config{
				Profile: "sync", WorkerLaunchDarklyFeatureFlagsEnabled: true,
			},
		},
		{
			name: "github repo metadata",
			cfg: config.Config{
				Profile: "sync", WorkerGithubRepoMetadataEnabled: true,
			},
		},
		{
			name: "gitlab repo metadata",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabRepoMetadataEnabled: true,
			},
		},
		{
			name: "gitlab commits",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabCommitsEnabled: true,
			},
		},
		{
			name: "gitlab commit stats",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabCommitStatsEnabled: true,
			},
		},
		{
			name: "gitlab cicd",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabCICDEnabled: true,
			},
		},
		{
			name: "gitlab tests",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabTestsEnabled: true,
			},
		},
		{
			name: "gitlab incidents",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabIncidentsEnabled: true,
			},
		},
		{
			name: "gitlab deployments",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabDeploymentsEnabled: true,
			},
		},
		{
			name: "gitlab feature flags",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabFeatureFlagsEnabled: true,
			},
		},
		{
			name: "gitlab files",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabFilesEnabled: true,
			},
		},
		{
			name: "gitlab blame",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabBlameEnabled: true,
			},
		},
		{
			name: "gitlab prs",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabPRsEnabled: true,
			},
		},
		{
			name: "gitlab pr reviews",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabPRReviewsEnabled: true,
			},
		},
		{
			name: "gitlab pr comments",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabPRCommentsEnabled: true,
			},
		},
		{
			name: "gitlab security",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabSecurityEnabled: true,
			},
		},
		{
			name: "gitlab work items",
			cfg: config.Config{
				Profile: "sync", WorkerGitlabWorkItemsEnabled: true,
			},
		},
		{
			name: "jira work items",
			cfg: config.Config{
				Profile: "sync", WorkerJiraWorkItemsEnabled: true,
			},
		},
		{
			name: "linear work items",
			cfg: config.Config{
				Profile: "sync", WorkerLinearWorkItemsEnabled: true,
			},
		},
		{
			name: "pagerduty services",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyServicesEnabled: true,
			},
		},
		{
			name: "pagerduty business services",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyBusinessServicesEnabled: true,
			},
		},
		{
			name: "pagerduty escalation policies",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyEscalationPoliciesEnabled: true,
			},
		},
		{
			name: "pagerduty schedules",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutySchedulesEnabled: true,
			},
		},
		{
			name: "pagerduty on calls",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyOnCallsEnabled: true,
			},
		},
		{
			name: "pagerduty users",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyUsersEnabled: true,
			},
		},
		{
			name: "pagerduty teams",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyTeamsEnabled: true,
			},
		},
		{
			name: "pagerduty incident family",
			cfg: config.Config{
				Profile: "sync", WorkerPagerDutyIncidentsEnabled: true,
			},
		},
		{
			name: "github cicd",
			cfg: config.Config{
				Profile: "sync", WorkerGithubCICDEnabled: true,
			},
		},
		{
			name: "github prs",
			cfg: config.Config{
				Profile: "sync", WorkerGithubPRsEnabled: true,
			},
		},
		{
			name: "github pr reviews",
			cfg: config.Config{
				Profile: "sync", WorkerGithubPRReviewsEnabled: true,
			},
		},
		{
			name: "github pr comments",
			cfg: config.Config{
				Profile: "sync", WorkerGithubPRCommentsEnabled: true,
			},
		},
		{
			name: "github commits",
			cfg: config.Config{
				Profile: "sync", WorkerGithubCommitsEnabled: true,
			},
		},
		{
			name: "github deployments",
			cfg: config.Config{
				Profile: "sync", WorkerGithubDeploymentsEnabled: true,
			},
		},
		{
			name: "github security",
			cfg: config.Config{
				Profile: "sync", WorkerGithubSecurityEnabled: true,
			},
		},
		{
			name: "github files",
			cfg: config.Config{
				Profile: "sync", WorkerGithubFilesEnabled: true,
			},
		},
		{
			name: "github commit stats",
			cfg: config.Config{
				Profile: "sync", WorkerGithubCommitStatsEnabled: true,
			},
		},
		{
			name: "github blame",
			cfg: config.Config{
				Profile: "sync", WorkerGithubBlameEnabled: true,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			family, err := buildProviderSyncWorker(
				context.Background(), test.cfg, nil, nil, nil, nil,
			)
			if err != nil {
				t.Fatalf("buildProviderSyncWorker() error = %v", err)
			}
			if len(family.queues) == 0 {
				t.Fatal("buildProviderSyncWorker returned an empty worker family")
			}
		})
	}
}

// TestWorkerRouteSwitchesMapsEveryConfiguredRoute proves the config->switches
// translation carries each flag to its own field. A copy/paste that pointed two
// fields at one config flag would still satisfy a single-flag test.
func TestWorkerRouteSwitchesMapsEveryConfiguredRoute(t *testing.T) {
	t.Parallel()
	if got := workerRouteSwitches(config.Config{}); got != (providersync.CompleteRouteSwitches{}) {
		t.Fatalf("zero config produced %+v", got)
	}
	for name, probe := range map[string]struct {
		cfg  config.Config
		want providersync.CompleteRouteSwitches
	}{
		"launchdarkly": {
			cfg:  config.Config{WorkerLaunchDarklyFeatureFlagsEnabled: true},
			want: providersync.CompleteRouteSwitches{LaunchDarklyFeatureFlags: true},
		},
		"github": {
			cfg:  config.Config{WorkerGithubRepoMetadataEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubRepoMetadata: true},
		},
		"gitlab": {
			cfg:  config.Config{WorkerGitlabRepoMetadataEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabRepoMetadata: true},
		},
		"gitlab_commits": {
			cfg:  config.Config{WorkerGitlabCommitsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabCommits: true},
		},
		"gitlab_commit_stats": {
			cfg:  config.Config{WorkerGitlabCommitStatsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabCommitStats: true},
		},
		"gitlab_cicd": {
			cfg:  config.Config{WorkerGitlabCICDEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabCICD: true},
		},
		"gitlab_tests": {
			cfg:  config.Config{WorkerGitlabTestsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabTests: true},
		},
		"gitlab_incidents": {
			cfg:  config.Config{WorkerGitlabIncidentsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabIncidents: true},
		},
		"gitlab_deployments": {
			cfg:  config.Config{WorkerGitlabDeploymentsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabDeployments: true},
		},
		"gitlab_feature_flags": {
			cfg:  config.Config{WorkerGitlabFeatureFlagsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabFeatureFlags: true},
		},
		"gitlab_files": {
			cfg:  config.Config{WorkerGitlabFilesEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabFiles: true},
		},
		"gitlab_blame": {
			cfg:  config.Config{WorkerGitlabBlameEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabBlame: true},
		},
		"gitlab_prs": {
			cfg:  config.Config{WorkerGitlabPRsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabPRs: true},
		},
		"gitlab_pr_reviews": {
			cfg:  config.Config{WorkerGitlabPRReviewsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabPRReviews: true},
		},
		"gitlab_pr_comments": {
			cfg:  config.Config{WorkerGitlabPRCommentsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabPRComments: true},
		},
		"gitlab_security": {
			cfg:  config.Config{WorkerGitlabSecurityEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabSecurity: true},
		},
		"gitlab_work_items": {
			cfg:  config.Config{WorkerGitlabWorkItemsEnabled: true},
			want: providersync.CompleteRouteSwitches{GitlabWorkItems: true},
		},
		"pagerduty_services": {
			cfg:  config.Config{WorkerPagerDutyServicesEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyServices: true},
		},
		"pagerduty_business_services": {
			cfg:  config.Config{WorkerPagerDutyBusinessServicesEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyBusinessServices: true},
		},
		"pagerduty_escalation_policies": {
			cfg:  config.Config{WorkerPagerDutyEscalationPoliciesEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyEscalationPolicies: true},
		},
		"pagerduty_schedules": {
			cfg:  config.Config{WorkerPagerDutySchedulesEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutySchedules: true},
		},
		"pagerduty_on_calls": {
			cfg:  config.Config{WorkerPagerDutyOnCallsEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyOnCalls: true},
		},
		"pagerduty_users": {
			cfg:  config.Config{WorkerPagerDutyUsersEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyUsers: true},
		},
		"pagerduty_teams": {
			cfg:  config.Config{WorkerPagerDutyTeamsEnabled: true},
			want: providersync.CompleteRouteSwitches{PagerDutyTeams: true},
		},
		"pagerduty_incident_family": {
			cfg: config.Config{WorkerPagerDutyIncidentsEnabled: true},
			want: providersync.CompleteRouteSwitches{
				PagerDutyIncidents: true, PagerDutyIncidentAlerts: true,
				PagerDutyIncidentLogEntries: true, PagerDutyIncidentNotes: true,
			},
		},
		"github_prs": {
			cfg:  config.Config{WorkerGithubPRsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubPRs: true},
		},
		"github_pr_reviews": {
			cfg:  config.Config{WorkerGithubPRReviewsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubPRReviews: true},
		},
		"github_pr_comments": {
			cfg:  config.Config{WorkerGithubPRCommentsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubPRComments: true},
		},
		"github_cicd": {
			cfg:  config.Config{WorkerGithubCICDEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubCICD: true},
		},
		"github_commits": {
			cfg:  config.Config{WorkerGithubCommitsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubCommits: true},
		},
		"github_deployments": {
			cfg:  config.Config{WorkerGithubDeploymentsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubDeployments: true},
		},
		"github_security": {
			cfg:  config.Config{WorkerGithubSecurityEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubSecurity: true},
		},
		"github_files": {
			cfg:  config.Config{WorkerGithubFilesEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubFiles: true},
		},
		"github_commit_stats": {
			cfg:  config.Config{WorkerGithubCommitStatsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubCommitStats: true},
		},
		"github_blame": {
			cfg:  config.Config{WorkerGithubBlameEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubBlame: true},
		},
		"github_tests": {
			cfg:  config.Config{WorkerGithubTestsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubTests: true},
		},
		"github_work_items": {
			cfg:  config.Config{WorkerGithubWorkItemsEnabled: true},
			want: providersync.CompleteRouteSwitches{GithubWorkItems: true},
		},
		"linear": {
			cfg:  config.Config{WorkerLinearWorkItemsEnabled: true},
			want: providersync.CompleteRouteSwitches{LinearWorkItems: true},
		},
		"jira_work_items": {
			cfg:  config.Config{WorkerJiraWorkItemsEnabled: true},
			want: providersync.CompleteRouteSwitches{JiraWorkItems: true},
		},
		"jira_incidents": {
			cfg:  config.Config{WorkerJiraIncidentsEnabled: true},
			want: providersync.CompleteRouteSwitches{JiraIncidents: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := workerRouteSwitches(probe.cfg); got != probe.want {
				t.Fatalf("workerRouteSwitches = %+v, want %+v", got, probe.want)
			}
		})
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubCommitStatsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GithubCommitStats: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "commit-stats",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubCommitStatsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubCommitStatsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubPRSocialAliases(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		t.Run(dataset, func(t *testing.T) {
			t.Parallel()
			handler, _ := buildProviderSyncHandler(
				nil, providersync.CompleteRouteSwitches{},
				nil, nil, nil, nil, nil, nil, slog.Default(),
			)
			executor, err := handler.BuildExecutor(&providersync.LeaseSession{
				Claim: providersync.Claim{Unit: providersync.Unit{
					Provider: "github", Dataset: dataset,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := executor.Handler.(providersync.GitHubPullRequestSocialRouteHandler); !ok {
				t.Fatalf("executor handler=%T", executor.Handler)
			}
			if _, ok := executor.Committer.Sink.(providersync.GitHubPullRequestSocialClickHouseEffects); !ok {
				t.Fatalf("executor sink=%T", executor.Committer.Sink)
			}
		})
	}
}

func TestBuildProviderSyncHandlerConstructsJiraIncidentsCapability(t *testing.T) {
	entitlementFunc := providerSyncEntitlementFunc(func(context.Context, string) error { return nil })
	entitlement := &entitlementFunc
	handler, _ := buildProviderSyncHandler(
		nil, providersync.CompleteRouteSwitches{JiraIncidents: true},
		nil, nil, nil, nil, entitlement, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "jira", Dataset: "incidents",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := executor.Handler.(providersync.JiraIncidentRouteHandler)
	if !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	sink, ok := executor.Committer.Sink.(providersync.JiraIncidentClickHouseEffects)
	if !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
	if route.Entitlement != entitlement || sink.Entitlement != entitlement {
		t.Fatal("Jira route and writer do not share the constructed entitlement")
	}
	if _, ok := executor.Committer.Readback.(providersync.JiraIncidentClickHouseReadback); !ok {
		t.Fatalf("executor readback=%T", executor.Committer.Readback)
	}
}

func TestBuildProviderSyncHandlerRejectsJiraIncidentsWithoutEntitlement(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil, providersync.CompleteRouteSwitches{JiraIncidents: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	_, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "jira", Dataset: "incidents",
		}},
	})
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("error=%v want dependency unavailable", err)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubBlameCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GithubBlame: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "blame",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	blameHandler, ok := executor.Handler.(providersync.GitHubBlameRouteHandler)
	if !ok || blameHandler.Coverage == nil {
		t.Fatalf("executor handler=%T coverage=%v", executor.Handler, blameHandler.Coverage)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubBlameClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubTestsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil, providersync.CompleteRouteSwitches{GithubTests: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "tests",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubTestsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubTestsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubCICDCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GithubCICD: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "cicd",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubTestsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubTestsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubCommitsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GithubCommits: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "github", Dataset: "commits",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubCommitsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubCommitsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubDeploymentsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(nil, providersync.CompleteRouteSwitches{GithubDeployments: true}, nil, nil, nil, nil, nil, nil, slog.Default())
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{Claim: providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "deployments"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubDeploymentsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubDeploymentsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubSecurityCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(nil, providersync.CompleteRouteSwitches{GithubSecurity: true}, nil, nil, nil, nil, nil, nil, slog.Default())
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{Claim: providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "security"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubSecurityRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubSecurityClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestBuildProviderSyncHandlerConstructsGitHubFilesCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(nil, providersync.CompleteRouteSwitches{GithubFiles: true}, nil, nil, nil, nil, nil, nil, slog.Default())
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{Claim: providersync.Claim{Unit: providersync.Unit{Provider: "github", Dataset: "files"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitHubFilesRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitHubFilesClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
}

func TestProviderSyncWorkerEnabledForEveryRouteReadySwitch(t *testing.T) {
	for name, cfg := range map[string]config.Config{
		"launchdarkly":         {WorkerLaunchDarklyFeatureFlagsEnabled: true},
		"github_repo_metadata": {WorkerGithubRepoMetadataEnabled: true},
		"gitlab_repo_metadata": {WorkerGitlabRepoMetadataEnabled: true},
		"gitlab_commits":       {WorkerGitlabCommitsEnabled: true},
		"gitlab_commit_stats":  {WorkerGitlabCommitStatsEnabled: true},
		"gitlab_cicd":          {WorkerGitlabCICDEnabled: true},
		"gitlab_tests":         {WorkerGitlabTestsEnabled: true},
		"gitlab_incidents":     {WorkerGitlabIncidentsEnabled: true},
		"gitlab_deployments":   {WorkerGitlabDeploymentsEnabled: true},
		"gitlab_feature_flags": {WorkerGitlabFeatureFlagsEnabled: true},
		"gitlab_files":         {WorkerGitlabFilesEnabled: true},
		"gitlab_blame":         {WorkerGitlabBlameEnabled: true},
		"gitlab_prs":           {WorkerGitlabPRsEnabled: true},
		"gitlab_pr_reviews":    {WorkerGitlabPRReviewsEnabled: true},
		"gitlab_pr_comments":   {WorkerGitlabPRCommentsEnabled: true},
		"gitlab_security":      {WorkerGitlabSecurityEnabled: true},
		"gitlab_work_items":    {WorkerGitlabWorkItemsEnabled: true},
		"jira_work_items":      {WorkerJiraWorkItemsEnabled: true},
		"linear_work_items":    {WorkerLinearWorkItemsEnabled: true},
		"pagerduty_services":   {WorkerPagerDutyServicesEnabled: true},
		"pagerduty_business_services": {
			WorkerPagerDutyBusinessServicesEnabled: true,
		},
		"pagerduty_escalation_policies": {
			WorkerPagerDutyEscalationPoliciesEnabled: true,
		},
		"pagerduty_schedules": {WorkerPagerDutySchedulesEnabled: true},
		"pagerduty_on_calls":  {WorkerPagerDutyOnCallsEnabled: true},
		"pagerduty_users":     {WorkerPagerDutyUsersEnabled: true},
		"pagerduty_teams":     {WorkerPagerDutyTeamsEnabled: true},
		"pagerduty_incidents": {WorkerPagerDutyIncidentsEnabled: true},
		"github_cicd":         {WorkerGithubCICDEnabled: true},
		"github_commits":      {WorkerGithubCommitsEnabled: true},
		"github_deployments":  {WorkerGithubDeploymentsEnabled: true},
		"github_security":     {WorkerGithubSecurityEnabled: true},
		"github_files":        {WorkerGithubFilesEnabled: true},
		"github_commit_stats": {WorkerGithubCommitStatsEnabled: true},
		"jira_incidents":      {WorkerJiraIncidentsEnabled: true},
		"github_blame":        {WorkerGithubBlameEnabled: true},
		"github_tests":        {WorkerGithubTestsEnabled: true},
		"github_work_items":   {WorkerGithubWorkItemsEnabled: true},
	} {
		t.Run(name, func(t *testing.T) {
			if !providerSyncWorkerEnabled(cfg) {
				t.Fatal("route-ready switch leaves the provider worker family dormant")
			}
		})
	}
}

func TestBuildProviderSyncHandlerConstructsGitLabRepositoryCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GitlabRepoMetadata: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "gitlab", Dataset: "repo-metadata",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitLabRepositoryRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitLabRepositoryClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
	if _, ok := executor.Committer.Readback.(providersync.GitLabRepositoryClickHouseEffects); !ok {
		t.Fatalf("executor readback=%T", executor.Committer.Readback)
	}
}

func TestBuildProviderSyncHandlerConstructsGitLabCommitsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GitlabCommits: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "gitlab", Dataset: "commits",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitLabCommitsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitLabCommitsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
	if _, ok := executor.Committer.Readback.(providersync.GitLabCommitsClickHouseEffects); !ok {
		t.Fatalf("executor readback=%T", executor.Committer.Readback)
	}
}

func TestBuildProviderSyncHandlerConstructsGitLabCommitStatsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
		providersync.CompleteRouteSwitches{GitlabCommitStats: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	if handler == nil || handler.BuildExecutor == nil {
		t.Fatal("provider sync handler is not constructed")
	}
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "gitlab", Dataset: "commit-stats",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitLabCommitStatsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitLabCommitStatsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
	if _, ok := executor.Committer.Readback.(providersync.GitLabCommitStatsClickHouseEffects); !ok {
		t.Fatalf("executor readback=%T", executor.Committer.Readback)
	}
}

func TestBuildProviderSyncHandlerConstructsGitLabCICDCapability(t *testing.T) {
	for _, test := range []struct {
		dataset  string
		switches providersync.CompleteRouteSwitches
	}{
		{dataset: "cicd", switches: providersync.CompleteRouteSwitches{GitlabCICD: true}},
		{dataset: "tests", switches: providersync.CompleteRouteSwitches{GitlabTests: true}},
	} {
		t.Run(test.dataset, func(t *testing.T) {
			handler, _ := buildProviderSyncHandler(
				nil, test.switches, nil, nil, nil, nil, nil, nil, slog.Default(),
			)
			if handler == nil || handler.BuildExecutor == nil {
				t.Fatal("provider sync handler is not constructed")
			}
			executor, err := handler.BuildExecutor(&providersync.LeaseSession{
				Claim: providersync.Claim{Unit: providersync.Unit{
					Provider: "gitlab", Dataset: test.dataset,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := executor.Handler.(providersync.GitLabTestsRouteHandler); !ok {
				t.Fatalf("executor handler=%T", executor.Handler)
			}
			if _, ok := executor.Committer.Sink.(providersync.TestOpsClickHouseEffects); !ok {
				t.Fatalf("executor sink=%T", executor.Committer.Sink)
			}
			if _, ok := executor.Committer.Readback.(providersync.TestOpsClickHouseEffects); !ok {
				t.Fatalf("executor readback=%T", executor.Committer.Readback)
			}
			client, ok := executor.Doer.(*http.Client)
			if !ok {
				t.Fatalf("executor doer=%T", executor.Doer)
			}
			if client.CheckRedirect == nil {
				t.Fatal("provider client must expose redirects to the route before an unauthenticated follow")
			}
			request, requestErr := http.NewRequest(http.MethodGet, "https://blob.example/artifact", nil)
			if requestErr != nil {
				t.Fatal(requestErr)
			}
			if redirectErr := client.CheckRedirect(request, nil); !errors.Is(redirectErr, http.ErrUseLastResponse) {
				t.Fatalf("redirect policy error=%v", redirectErr)
			}
		})
	}
}

func TestBuildProviderSyncHandlerConstructsGitLabIncidentsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil, providersync.CompleteRouteSwitches{GitlabIncidents: true},
		nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "gitlab", Dataset: "incidents",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.Handler.(providersync.GitLabIncidentsRouteHandler); !ok {
		t.Fatalf("executor handler=%T", executor.Handler)
	}
	if _, ok := executor.Committer.Sink.(providersync.GitLabIncidentsClickHouseEffects); !ok {
		t.Fatalf("executor sink=%T", executor.Committer.Sink)
	}
	if _, ok := executor.Committer.Readback.(providersync.GitLabIncidentsClickHouseEffects); !ok {
		t.Fatalf("executor readback=%T", executor.Committer.Readback)
	}
}
