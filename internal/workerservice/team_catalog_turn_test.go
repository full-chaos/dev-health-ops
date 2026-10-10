package workerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type recordingBudget struct {
	requests []jobruntime.BudgetRequest
	err      error
	released int
}

func (budget *recordingBudget) Acquire(_ context.Context, request jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
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
	turn := catalogLeaseTurn{budget: budget}
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
	if _, err := newCatalogLeaseTurn(nil).Serialize(context.Background(), "20000000-0000-4000-8000-000000000001", "linear"); err == nil {
		t.Error("a turn without a Postgres pool served a run")
	}
	failed := errors.New("store down")
	if _, err := (catalogLeaseTurn{budget: &recordingBudget{err: failed}}).Serialize(context.Background(), "20000000-0000-4000-8000-000000000001", "linear"); !errors.Is(err, failed) {
		t.Errorf("err = %v, want the store's error wrapped", err)
	}
}
