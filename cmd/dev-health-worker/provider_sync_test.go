package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/riverqueue/river"
)

type providerSyncEntitlementFunc func(context.Context, string) error

func TestWorkerCredentialCipherUsesConfiguredSettingsEncryptionSalt(t *testing.T) {
	t.Parallel()
	cipher, err := newWorkerCredentialCipher(config.Config{
		SettingsEncryptionKey:  secrets.NewValue("test-master-key"),
		SettingsEncryptionSalt: secrets.NewValue("deployment-specific-salt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt([]byte("pagerduty-oauth-token"))
	if err != nil {
		t.Fatal(err)
	}
	defaultCipher, err := providerfoundation.NewFernetDecryptor(
		secrets.NewValue("test-master-key"), "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := defaultCipher.Decrypt(ciphertext); err == nil {
		t.Fatal("custom-salt ciphertext decrypted with the default salt")
	}
	plaintext, err := cipher.Decrypt(ciphertext)
	if err != nil || string(plaintext) != "pagerduty-oauth-token" {
		t.Fatalf("custom-salt round trip: plaintext=%q err=%v", plaintext, err)
	}
}

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
				nil, nil, nil, nil, nil,
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
		nil, nil,
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
		nil, nil,
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
				nil, nil, nil, nil, nil,
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

func TestBuildProviderSyncHandlerWiresPagerDutyCredentialHydrator(t *testing.T) {
	t.Parallel()
	hydrator := providerSyncCredentialHydratorFunc(func(
		_ context.Context,
		_ providerfoundation.LeaseGuard,
		_ providerfoundation.TenantScope,
		credential providerfoundation.Credential,
	) (providerfoundation.Credential, error) {
		return credential, nil
	})
	handler, _ := buildProviderSyncHandlerWithRuntimeDependencies(
		nil, nil, hydrator,
		nil, nil, nil, nil, nil, slog.Default(), workItemsRuntimeConfig{},
	)
	executor, err := handler.BuildExecutor(&providersync.LeaseSession{
		Claim: providersync.Claim{Unit: providersync.Unit{
			Provider: "pagerduty", Dataset: "services",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if executor.Credentials.Hydrator == nil {
		t.Fatal("PagerDuty OAuth credential hydrator was not wired")
	}
}

type providerSyncCredentialHydratorFunc func(
	context.Context,
	providerfoundation.LeaseGuard,
	providerfoundation.TenantScope,
	providerfoundation.Credential,
) (providerfoundation.Credential, error)

func (hydrate providerSyncCredentialHydratorFunc) Hydrate(
	ctx context.Context,
	lease providerfoundation.LeaseGuard,
	scope providerfoundation.TenantScope,
	credential providerfoundation.Credential,
) (providerfoundation.Credential, error) {
	return hydrate(ctx, lease, scope, credential)
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
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	// Every dependency below is only ever stored as a closure capture inside
	// buildProviderSyncHandler — never dialed, dereferenced, or called during
	// construction — so nil stand-ins are sufficient to reach the real,
	// unmodified BuildExecutor closure without a live ClickHouse, Valkey, or
	// Postgres connection.
	handler, providerMetrics := buildProviderSyncHandler(
		nil, nil, nil, nil, nil,
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

func TestBuildProviderSyncWorkerFollowsQueueTopology(t *testing.T) {
	originalConstructor := constructProviderSyncWorker
	t.Cleanup(func() {
		constructProviderSyncWorker = originalConstructor
	})

	// CHAOS-4054: capability is always on in the binary. buildProviderSyncWorker's
	// only gate is the -Q queue topology -- the family is constructed whenever the
	// provider-unit queue is selected, and never constructed when it is not, with
	// no WORKER_*_ENABLED flag able to change that either way.
	constructed := false
	constructProviderSyncWorker = func(
		_ context.Context,
		_ config.Config,
		_ workerDatabase,
		_ *jobruntime.Registry,
		_ jobruntime.Observer,
		_ *slog.Logger,
		_ *river.Workers,
	) (workerFamily, error) {
		constructed = true
		return workerFamily{
			queues: []jobruntime.QueueBudget{{
				Queue: providerUnitQueue, MaxWorkers: 17,
			}},
		}, nil
	}

	family, err := buildProviderSyncWorker(
		context.Background(), config.Config{Queues: []string{"sync"}},
		nil, nil, nil, nil, river.NewWorkers(),
	)
	if err != nil {
		t.Fatalf("buildProviderSyncWorker() error = %v", err)
	}
	if constructed || len(family.queues) != 0 {
		t.Fatalf("queue not selected: constructed=%v family=%+v, want no construction", constructed, family)
	}

	family, err = buildProviderSyncWorker(
		context.Background(), config.Config{Queues: []string{providerUnitQueue}},
		nil, nil, nil, nil, river.NewWorkers(),
	)
	if err != nil {
		t.Fatalf("buildProviderSyncWorker() error = %v", err)
	}
	if !constructed || len(family.queues) == 0 {
		t.Fatalf("queue selected: constructed=%v family=%+v, want the constructed family", constructed, family)
	}
}

// TestCompleteWriterAliasesAreRouteReadyButNotPlannable proves the CHAOS-4054
// alias contract for the writer-collapsed identities: github/gitlab
// pr-reviews and pr-comments (canonical: prs) and github/gitlab tests
// (canonical: cicd) stay RouteReady -- BuildExecutor still resolves them to
// the shared canonical handler, since a Python producer may still route a
// unit to one of these historical identities -- but they are never
// independently Plannable. Only the canonical writer is.
func TestCompleteWriterAliasesAreRouteReadyButNotPlannable(t *testing.T) {
	t.Parallel()
	handler, _ := buildProviderSyncHandler(
		nil, nil, nil, nil, nil, nil, nil, slog.Default(),
	)
	for _, route := range []struct {
		provider string
		dataset  string
	}{
		{"github", "pr-reviews"}, {"github", "pr-comments"}, {"github", "tests"},
		{"gitlab", "pr-reviews"}, {"gitlab", "pr-comments"}, {"gitlab", "tests"},
	} {
		descriptor, ok := providersync.Descriptor(route.provider, route.dataset)
		if !ok || !descriptor.RouteReady || descriptor.Plannable {
			t.Fatalf(
				"%s/%s descriptor=%+v ok=%v, want RouteReady && !Plannable",
				route.provider, route.dataset, descriptor, ok,
			)
		}
		executor, err := handler.BuildExecutor(&providersync.LeaseSession{
			Claim: providersync.Claim{Unit: providersync.Unit{
				Provider: route.provider, Dataset: route.dataset,
			}},
		})
		if err != nil {
			t.Fatalf("%s/%s executor: %v", route.provider, route.dataset, err)
		}
		if executor.Handler == nil {
			t.Fatalf("%s/%s executor has no complete handler", route.provider, route.dataset)
		}
	}
}
func TestBuildProviderSyncHandlerConstructsGitHubCommitStatsCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
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
				nil, nil, nil, nil, nil, nil, nil, slog.Default(),
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
		nil, nil, nil, nil, nil, entitlement, nil, slog.Default(),
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
		nil, nil, nil, nil, nil, nil, nil, slog.Default(),
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
		nil, nil, nil, nil, nil, nil, nil, slog.Default(),
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
	handler, _ := buildProviderSyncHandler(nil, nil, nil, nil, nil, nil, nil, slog.Default())
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
	handler, _ := buildProviderSyncHandler(nil, nil, nil, nil, nil, nil, nil, slog.Default())
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
	handler, _ := buildProviderSyncHandler(nil, nil, nil, nil, nil, nil, nil, slog.Default())
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

func TestBuildProviderSyncHandlerConstructsGitLabRepositoryCapability(t *testing.T) {
	handler, _ := buildProviderSyncHandler(
		nil,
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
		dataset string
	}{
		{dataset: "cicd"},
		{dataset: "tests"},
	} {
		t.Run(test.dataset, func(t *testing.T) {
			handler, _ := buildProviderSyncHandler(
				nil, nil, nil, nil, nil, nil, nil, slog.Default(),
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
		nil, nil, nil, nil, nil, nil, nil, slog.Default(),
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
