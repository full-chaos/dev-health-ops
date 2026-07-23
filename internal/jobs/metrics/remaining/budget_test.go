package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestBudgetEnforcesFamilyClickHouseCeiling(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewBudget(inventory)
	if err != nil {
		t.Fatal(err)
	}
	organizationID := "00000000-0000-4000-8000-000000000109"
	request := jobruntime.BudgetRequest{
		Kind:             jobcontract.KindRemainingComplexity,
		OrganizationID:   &organizationID,
		ConcurrencyScope: "organization",
		ConcurrencyLimit: 2,
	}
	first, err := budget.Acquire(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := budget.Acquire(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if lease, err := budget.Acquire(ctx, request); err == nil || lease != nil {
		t.Fatalf("third complexity lease bypassed ClickHouse budget: %#v %v", lease, err)
	}
}

func TestBudgetRejectsDescriptorInventoryDrift(t *testing.T) {
	inventory, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewBudget(inventory)
	if err != nil {
		t.Fatal(err)
	}
	organizationID := "00000000-0000-4000-8000-000000000109"
	request := jobruntime.BudgetRequest{
		Kind:             jobcontract.KindRemainingDORA,
		OrganizationID:   &organizationID,
		ConcurrencyScope: "organization",
		ConcurrencyLimit: 3,
	}
	if lease, err := budget.Acquire(t.Context(), request); err == nil || lease != nil {
		t.Fatalf("drifted descriptor accepted: %#v %v", lease, err)
	}
}
