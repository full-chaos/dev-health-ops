package workerservice

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The GitHub tests/cicd artifact prefetch waits on its own sub-budget class (CHAOS-14542). The worker registers that
// class for github only, so its wait reaches worker_budget_wait_seconds instead of being dropped as unregistered.
func TestBudgetDimensionsRegisterTheGitHubArtifactPrefetchClass(t *testing.T) {
	labels := budgetDimensionsForQueues([]string{providerUnitQueue})
	prefetch, ok := providersync.PrefetchBudgetCostClass(providersync.CostMedium)
	if !ok || prefetch != "medium-prefetch" {
		t.Fatalf("PrefetchBudgetCostClass(medium) = %q, %v; want medium-prefetch, true", prefetch, ok)
	}
	want := jobruntime.BudgetLabels{Provider: "github", CostClass: string(prefetch)}
	found := 0
	for _, label := range labels {
		if label == want {
			found++
		}
		if label.CostClass == string(prefetch) && label.Provider != "github" {
			t.Fatalf("prefetch class registered for %q: only the GitHub route prefetches", label.Provider)
		}
	}
	if found != 1 {
		t.Fatalf("%v registered %d times, want once", want, found)
	}
	if got, base := len(labels), len(providersync.MatrixProviders())*3+1; got != base {
		t.Fatalf("%d budget labels, want %d (providers x light/medium/heavy + github medium-prefetch)", got, base)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{Budgets: labels})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveProviderBudgetWait(want, time.Millisecond); err != nil {
		t.Fatalf("observe %v: %v, want it recorded", want, err)
	}
	if err := collector.ObserveProviderBudgetWait(jobruntime.BudgetLabels{Provider: "gitlab", CostClass: string(prefetch)}, time.Millisecond); err == nil {
		t.Fatal("gitlab medium-prefetch was recorded: it is not a registered dimension")
	}
	if labels := budgetDimensionsForQueues([]string{"sync"}); len(labels) != 0 {
		t.Fatalf("a process without the provider-unit queue registered %d budget labels, want 0", len(labels))
	}
}
