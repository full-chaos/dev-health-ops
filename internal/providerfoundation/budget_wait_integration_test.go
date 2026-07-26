//go:build integration

package providerfoundation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestValkeyBudgetStoreAndBackoffGateObserveRealWait is CHAOS-3118 evidence
// for worker_budget_wait_seconds: it runs ValkeyBudgetStore.Acquire and
// ValkeyBackoffGate.Wait against a real, isolated Valkey instance (not a
// mocked client), wired to a real *jobruntime.MetricsCollector exactly the
// way cmd/dev-health-worker/provider_sync.go wires them in production, and
// reads the collector's own Prometheus exposition to prove non-zero series.
func TestValkeyBudgetStoreAndBackoffGateObserveRealWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{
		Profiles: []string{"sync"},
		Budgets: []jobruntime.BudgetLabels{
			{Provider: "github", CostClass: "medium"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	store := providerfoundation.ValkeyBudgetStore{Client: client, Observer: collectorBudgetObserver{collector}}
	key := providerfoundation.BudgetKey{
		Provider: "github", OrgID: "org-1", Host: "api.github.com",
		CostClass: "medium", Limit: 1, TTL: time.Minute,
	}
	reservation, err := store.Acquire(ctx, key)
	if err != nil || reservation == nil {
		t.Fatalf("Acquire() = %v, %v", reservation, err)
	}
	t.Cleanup(func() { _ = reservation.Release(context.Background()) })

	gate := providerfoundation.ValkeyBackoffGate{
		Client: client, Provider: "github", OrgID: "org-1", Host: "api.github.com",
		MaxBackoff: time.Minute, CostClass: "medium",
		Observer: collectorBudgetObserver{collector},
	}
	// Penalize's Lua script now returns tostring(applied) (CHAOS-3132), so
	// this exercises the real, unbroken Penalize -> Wait round trip against a
	// real server instead of hand-seeding the key it documents itself as
	// writing.
	if err := gate.Penalize(ctx, 2*time.Second); err != nil {
		t.Fatalf("Penalize: %v", err)
	}
	wait, err := gate.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if wait <= 0 {
		t.Fatalf("Wait() = %v, want a positive backoff", wait)
	}

	text := collector.PrometheusText()
	if !strings.Contains(text, `worker_budget_wait_seconds_count{provider="github",cost_class="medium"} 2`) {
		t.Fatalf("expected two non-zero worker_budget_wait_seconds observations (Acquire + Wait), got:\n%s", text)
	}
	if strings.Contains(text, `worker_budget_wait_seconds_sum{provider="github",cost_class="medium"} 0`) {
		t.Fatalf("expected a non-zero wait sum from the real Penalize/Wait round trip, got:\n%s", text)
	}
}

type collectorBudgetObserver struct {
	collector *jobruntime.MetricsCollector
}

func (o collectorBudgetObserver) ObserveProviderBudgetWait(provider, costClass string, wait time.Duration) error {
	return o.collector.ObserveProviderBudgetWait(jobruntime.BudgetLabels{Provider: provider, CostClass: costClass}, wait)
}
