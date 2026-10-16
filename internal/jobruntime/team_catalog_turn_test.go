package jobruntime

import (
	"context"
	"errors"
	"testing"
)

type recordingBudget struct {
	requests []BudgetRequest
	err      error
	released int
}

func (budget *recordingBudget) Acquire(_ context.Context, request BudgetRequest) (BudgetLease, error) {
	budget.requests = append(budget.requests, request)
	if budget.err != nil {
		return nil, budget.err
	}
	return releaseCounter{budget}, nil
}

type releaseCounter struct{ budget *recordingBudget }

func (lease releaseCounter) Release() { lease.budget.released++ }

// CHAOS-9140 (D5917): the turn of one run is ONE lease per (organization, provider) with a limit of 1,
// in the organization scope, released by the function it returns.
func TestCatalogLeaseTurnAsksForOneLeasePerOrganizationAndProvider(t *testing.T) {
	budget := &recordingBudget{}
	turn := TeamCatalogTurn{budget: budget}
	release, err := turn.Serialize(context.Background(), "20000000-0000-4000-8000-000000000001", "github")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if len(budget.requests) != 1 || budget.released != 1 {
		t.Fatalf("requests = %d, released = %d, want 1 and 1", len(budget.requests), budget.released)
	}
	request := budget.requests[0]
	if request.Kind != "team_catalog.github" || request.ConcurrencyScope != "organization" || request.ConcurrencyLimit != 1 ||
		request.OrganizationID == nil || *request.OrganizationID != "20000000-0000-4000-8000-000000000001" {
		t.Errorf("request = %+v, want kind team_catalog.github, organization scope, limit 1, the organization", request)
	}
	if len(request.Kind) > 96 {
		t.Errorf("the lease kind is %d characters, the table holds 96", len(request.Kind))
	}
}

func TestCatalogLeaseTurnFailsLoudlyWithoutAPoolOrALease(t *testing.T) {
	if _, err := NewTeamCatalogTurn(nil).Serialize(context.Background(), "20000000-0000-4000-8000-000000000001", "linear"); err == nil {
		t.Error("a turn without a Postgres pool served a run")
	}
	failed := errors.New("store down")
	if _, err := (TeamCatalogTurn{budget: &recordingBudget{err: failed}}).Serialize(context.Background(), "20000000-0000-4000-8000-000000000001", "linear"); !errors.Is(err, failed) {
		t.Errorf("err = %v, want the store's error wrapped", err)
	}
}

type lostLease struct {
	releaseCounter
	lost chan struct{}
}

func (l lostLease) Lost() <-chan struct{} { return l.lost }

type lossBudget struct{ lost chan struct{} }

func (b lossBudget) Acquire(context.Context, BudgetRequest) (BudgetLease, error) {
	return lostLease{releaseCounter{&recordingBudget{}}, b.lost}, nil
}

// Round 117 P1: the loss signal of the budget lease reaches the carry wrapper.
func TestCatalogLeaseTurnPassesTheLeaseLossSignal(t *testing.T) {
	lost := make(chan struct{})
	release, got, err := (TeamCatalogTurn{budget: lossBudget{lost}}).SerializeWithLoss(context.Background(), "20000000-0000-4000-8000-000000000001", "linear")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if got == nil {
		t.Fatal("no loss signal for a lease that has one")
	}
	close(lost)
	select {
	case <-got:
	default:
		t.Error("the signal is not the lease's own")
	}
	if _, none, _ := (TeamCatalogTurn{budget: &recordingBudget{}}).SerializeWithLoss(context.Background(), "20000000-0000-4000-8000-000000000001", "linear"); none != nil {
		t.Error("a lease that cannot be lost gave a signal")
	}
}
