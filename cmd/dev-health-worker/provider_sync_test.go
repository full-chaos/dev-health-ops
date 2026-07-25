package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

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
		collector, slog.Default(),
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
		"none":   {},
		"github": {GithubRepoMetadata: true},
		"both":   {GithubRepoMetadata: true, LaunchDarklyFeatureFlags: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			handler, _ := buildProviderSyncHandler(
				nil, want, nil, nil, nil, nil, nil, slog.Default(),
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
