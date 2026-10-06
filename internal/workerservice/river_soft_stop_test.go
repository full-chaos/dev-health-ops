package workerservice

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// TestComposedRiverProcessGetsTheDrainBudgetAsItsSoftStop (CHAOS-8783): for
// every deployed worker group -- queue set and --shutdown-timeout exactly as
// values.prod.yaml / go-workers.yaml render them (the chart passes
// terminationGracePeriodSeconds straight to --shutdown-timeout) -- the family
// the REAL composition hands to the River process builder carries a soft stop
// equal to the drain budget (shutdown timeout minus the named finalization
// buffer), and the component is stopped with the whole shutdown timeout so a
// job cancelled at the end of the soft stop still has the buffer to release.
func TestComposedRiverProcessGetsTheDrainBudgetAsItsSoftStop(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for _, group := range []struct {
		name     string
		queues   []string
		shutdown time.Duration
		wantSoft time.Duration
		// unset models the compose/default path: no --shutdown-timeout, so the
		// 30s config default is replaced by the derived requirement.
		unset bool
		// tooSmall is an explicit shutdown timeout below the drain contract.
		tooSmall bool
	}{
		{name: "heavy", queues: []string{"investment", "metrics", "reports", "workgraph"}, shutdown: 7260 * time.Second, wantSoft: 7200 * time.Second},
		{name: "heavy-unset", queues: []string{"investment", "metrics", "reports", "workgraph"}, shutdown: config.DefaultShutdownTimeout, wantSoft: 7200 * time.Second, unset: true},
		{name: "heavy-too-small", queues: []string{"investment", "metrics", "reports", "workgraph"}, shutdown: 600 * time.Second, tooSmall: true},
		{name: "ops", queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, shutdown: 960 * time.Second, wantSoft: 900 * time.Second},
		{name: "sync", queues: []string{"sync"}, shutdown: 960 * time.Second, wantSoft: 900 * time.Second},
		{name: "sync-provider", queues: []string{"sync_provider"}, shutdown: 960 * time.Second, wantSoft: 900 * time.Second},
	} {
		t.Run(group.name, func(t *testing.T) {
			// The checked-in contract itself (not the demoted test fixture):
			// the queue sets below are the deployed ones, and heavy's
			// investment and workgraph kinds are executable in it.
			contractRoot := defaultContractRoot
			runtimeRegistry, err := jobruntime.Load(contractRoot)
			if err != nil {
				t.Fatal(err)
			}
			sources := productionWorkerDependencySources
			sources.contractRoot = contractRoot
			sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
				return &fakeWorkerDatabase{}, nil
			}
			sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) { return runtimeRegistry, nil }
			var seen workerFamily
			sources.buildRiverProcess = func(
				_ config.Config, _ workerDatabase, _ *river.Workers, family workerFamily, _ *slog.Logger,
			) (lifecycle.Component, error) {
				seen = family
				return namedComponent("river-worker"), nil
			}
			concurrency := map[string]int{}
			for _, queue := range group.queues {
				concurrency[queue] = 1
			}
			specs := mustSelectedQueueSpecs(t, runtimeRegistry, group.queues...)
			budgets := make([]jobruntime.QueueBudget, 0, len(group.queues))
			for _, queue := range group.queues {
				budgets = append(budgets, jobruntime.QueueBudget{Queue: queue, MaxWorkers: 1})
			}
			sources.buildReports, sources.buildDaily, sources.buildProviderSync = nil, nil, nil
			sources.buildSyncCoordinator, sources.buildWorkgraph = nil, nil
			sources.buildOperational = fakeHandlerBuilder(group.name, specs, budgets...)

			components, err := configureWorkerDependenciesWithSources(
				context.Background(),
				config.Config{
					Queues: group.queues, WorkerQueueConcurrency: concurrency,
					WorkerGroup: group.name, ShutdownTimeout: group.shutdown, ShutdownTimeoutExplicit: !group.unset,
					RiverDatabaseSchema: "river", DomainDatabaseMaxConns: 4, QueueDatabaseMaxConns: 2,
				},
				health.NewRegistry(time.Second),
				sources,
			)
			if group.tooSmall {
				// A shutdown timeout below the longest job + buffer must stay a
				// startup error: it is what keeps the soft stop above zero.
				if err == nil || !strings.Contains(err.Error(), reasonShutdownTimeoutBelowDrainBudget) &&
					!strings.Contains(err.Error(), "worker_startup_contract_failed") {
					t.Fatalf("%s: compose = %v, want the shutdown-timeout startup error", group.name, err)
				}
				if seen.softStop != 0 {
					t.Fatalf("%s: a River process was built with soft stop %s despite the startup error", group.name, seen.softStop)
				}
				return
			}
			if err != nil {
				t.Fatalf("compose %s: %v", group.name, err)
			}
			wantShutdown := group.shutdown
			if group.unset {
				wantShutdown = group.wantSoft + workerFinalizationBuffer
			}
			if seen.softStop != group.wantSoft {
				t.Fatalf("%s: soft stop = %s, want the drain budget %s", group.name, seen.softStop, group.wantSoft)
			}
			process, ok := components[len(components)-1].(workerProcessComponent)
			if !ok || process.ShutdownBudget() != wantShutdown {
				t.Fatalf("%s: river-workers stop budget = %v, want the whole shutdown timeout %s",
					group.name, components[len(components)-1], wantShutdown)
			}
			if wantShutdown-seen.softStop != workerFinalizationBuffer {
				t.Fatalf("%s: slack after the soft stop = %s, want exactly the finalization buffer %s",
					group.name, wantShutdown-seen.softStop, workerFinalizationBuffer)
			}
		})
	}
}

// River reads a zero SoftStopTimeout as "off", which is the defect itself, so
// the process builder refuses to be built without one.
func TestRiverWorkerProcessRefusesAMissingSoftStop(t *testing.T) {
	database := &postgresWorkerDatabase{pools: nil}
	family := workerFamily{queues: []jobruntime.QueueBudget{{Queue: "heartbeat", MaxWorkers: 1}}}
	if _, err := newRiverWorkerProcess(config.Config{WorkerInstanceID: "w"}, database, river.NewWorkers(), family, slog.Default()); err == nil {
		t.Fatal("built a river process with no soft stop")
	}
}

func TestRiverWorkerClientConfigCarriesTheSoftStop(t *testing.T) {
	clientConfig := riverWorkerClientConfig(
		config.Config{WorkerInstanceID: "w", RiverDatabaseSchema: "river"},
		map[string]river.QueueConfig{"heartbeat": {MaxWorkers: 1}}, river.NewWorkers(), slog.Default(), 7200*time.Second)
	if clientConfig.SoftStopTimeout != 7200*time.Second {
		t.Fatalf("SoftStopTimeout = %s", clientConfig.SoftStopTimeout)
	}
}
