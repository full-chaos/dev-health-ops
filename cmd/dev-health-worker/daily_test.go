package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// TestMetricsCollectorFromObserverUnwrapsTheProductionObserver pins the
// round-2 codex finding (#2177, CHAOS-4282): the observer buildDailyWorker
// actually receives in production is claimLivenessObserver, not
// *jobruntime.MetricsCollector directly -- an exact-type assertion alone
// silently fails on it (embedding promotes methods, not concrete type
// identity), so any wiring that skips the Unwrap fallback never fires in
// production despite passing every unit test built against a bare collector.
func TestMetricsCollectorFromObserverUnwrapsTheProductionObserver(t *testing.T) {
	t.Parallel()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}

	// The bare collector case (what every existing unit test constructs).
	if got := metricsCollectorFromObserver(collector); got != collector {
		t.Fatalf("bare collector: got %v, want the same collector", got)
	}

	// The production shape: claimLivenessObserver wraps the collector.
	wrapped := claimLivenessObserver{MetricsCollector: collector, liveness: &claimLiveness{}}
	if got := metricsCollectorFromObserver(wrapped); got != collector {
		t.Fatalf("wrapped observer: got %v, want the exact embedded collector %v -- "+
			"the Unwrap fallback did not fire", got, collector)
	}

	// An observer with neither shape resolves to nil, not a panic.
	if got := metricsCollectorFromObserver(fakeObserverWithNoCollector{}); got != nil {
		t.Fatalf("unrelated observer: got %v, want nil", got)
	}
}

// fakeObserverWithNoCollector satisfies jobruntime.Observer by embedding the
// (nil) interface -- never invoked here, only type-asserted against -- and
// has neither the concrete *MetricsCollector type nor an Unwrap method: the
// "neither shape" case above.
type fakeObserverWithNoCollector struct {
	jobruntime.Observer
}

func TestRemainingFamilyDescriptorsMatchIndependentRoutesAndBudgets(t *testing.T) {
	t.Chdir("../..")
	registry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := remaining.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range inventory.Families {
		descriptor, ok := registry.Descriptor(family.RouteKey)
		if !ok {
			t.Fatalf("missing descriptor for %s", family.Name)
		}
		if err := validateRemainingFamilyDescriptor(family, descriptor); err != nil {
			t.Fatalf("%s descriptor drift: %v", family.Name, err)
		}

		promotedFamily := family
		promotedFamily.Route = "river"
		promotedFamily.RollbackRoute = "celery"
		promotedDescriptor := descriptor
		promotedDescriptor.Route = "river"
		promotedDescriptor.RollbackRoute = "celery"
		promotedDescriptor.MigrationState = "go_default"
		if err := validateRemainingFamilyDescriptor(promotedFamily, promotedDescriptor); err != nil {
			t.Fatalf("%s cannot be independently promoted: %v", family.Name, err)
		}
	}
}

func TestHeavyMetricsQueueFitsReviewedPostgresPools(t *testing.T) {
	t.Chdir("../..")
	contracts, err := jobcontract.LoadRegistry(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _, err := deploymentcontract.Load("deploy/go-workers/deployment.json", contracts)
	if err != nil {
		t.Fatal(err)
	}
	var process deploymentcontract.Process
	for _, candidate := range manifest.Processes {
		if candidate.Name == "heavy" {
			process = candidate
			break
		}
	}
	if process.Name == "" {
		t.Fatal("metrics worker group is missing")
	}
	var metricsWorkers int
	for _, queue := range process.QueueWorkers {
		if queue.Queue == "metrics" {
			metricsWorkers = queue.MaxWorkers
		}
	}
	if metricsWorkers < 1 ||
		metricsWorkers > process.DomainMaxConnections ||
		metricsWorkers > process.QueueControlMaxConnections {
		t.Fatalf(
			"metrics workers=%d domain_pool=%d queue_pool=%d",
			metricsWorkers,
			process.DomainMaxConnections,
			process.QueueControlMaxConnections,
		)
	}
}
