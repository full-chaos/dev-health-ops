package main

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

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
