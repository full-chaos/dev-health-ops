package jobruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConcurrencyBudgetKeyScopesIdentityWithoutPayload(t *testing.T) {
	organizationID := uuid.NewString()
	key, err := concurrencyBudgetKey(BudgetRequest{
		Kind: "investment.chunk", OrganizationID: &organizationID,
		ConcurrencyScope: "organization", ConcurrencyLimit: 1,
	})
	if err != nil || key != "investment.chunk|organization|"+organizationID {
		t.Fatalf("key = %q, err = %v", key, err)
	}
	fleet, err := concurrencyBudgetKey(BudgetRequest{Kind: "system.heartbeat", ConcurrencyScope: "fleet", ConcurrencyLimit: 1})
	if err != nil || fleet != "system.heartbeat|fleet|global" {
		t.Fatalf("fleet key = %q, err = %v", fleet, err)
	}
	for _, request := range []BudgetRequest{
		{Kind: "", ConcurrencyScope: "fleet", ConcurrencyLimit: 1},
		{Kind: "system.heartbeat", ConcurrencyScope: "fleet", ConcurrencyLimit: 1, OrganizationID: &organizationID},
		{Kind: "system.heartbeat", ConcurrencyScope: "organization", ConcurrencyLimit: 1},
	} {
		if _, err := concurrencyBudgetKey(request); err == nil {
			t.Fatalf("invalid request produced a key: %#v", request)
		}
	}
}

func TestConcurrencyBudgetMetricsExposeCapacityLeaseWaitAndRecovery(t *testing.T) {
	labels := ConcurrencyBudgetLabels{Kind: "investment.chunk", Scope: "organization"}
	collector, err := NewMetricsCollector(MetricDimensions{
		ConcurrencyBudgets: []ConcurrencyBudgetLabels{labels},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.SetConcurrencyBudgetCapacity(labels, 1); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetConcurrencyBudgetLeased(labels, 1); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveConcurrencyBudgetWait(labels, 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveConcurrencyBudgetExpiry(labels, "expired"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveConcurrencyBudgetExpiry(labels, "recovered"); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, metric := range []string{
		"worker_concurrency_budget_capacity{kind=\"investment.chunk\",scope=\"organization\"} 1",
		"worker_concurrency_budget_leased{kind=\"investment.chunk\",scope=\"organization\"} 1",
		"worker_concurrency_budget_wait_seconds_count{kind=\"investment.chunk\",scope=\"organization\"} 1",
		"worker_concurrency_budget_lease_events_total{kind=\"investment.chunk\",scope=\"organization\",result=\"expired\"} 1",
		"worker_concurrency_budget_lease_events_total{kind=\"investment.chunk\",scope=\"organization\",result=\"recovered\"} 1",
	} {
		if !strings.Contains(text, metric) {
			t.Errorf("metrics missing %q:\n%s", metric, text)
		}
	}
}

func TestPostgresConcurrencyBudgetRequiresPool(t *testing.T) {
	if _, err := NewPostgresConcurrencyBudget(nil, nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	var store *PostgresConcurrencyBudget
	if store.Supports("fleet", 1) {
		t.Fatal("nil store supports a budget")
	}
	if _, err := store.Acquire(context.Background(), BudgetRequest{Kind: "test", ConcurrencyScope: "fleet", ConcurrencyLimit: 1}); err == nil {
		t.Fatal("nil store acquired a lease")
	}
}
