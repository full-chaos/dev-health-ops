//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The finish stores the unit's result whole. The estimates the dispatch pass
// wrote come back through the claim as the database decodes them, are put on the
// payload with the comparison, and are still on the unit after the real Complete.
func TestPostgresCompleteKeepsTheBudgetEstimatesAndStoresTheComparison(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startLedgerPostgres(t, ctx)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET result = $2::jsonb WHERE id = $1::uuid`, firstUnitID,
		`{"keep":"me","observations":{"budget_estimate":[{"route_family":"commits","dimension":"rest_core","estimated_units":7,"confidence":"high"}]}}`); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, time.July, 23, 12, 1, 40, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: startedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"records": 7}
	snapshot := RequestLedgerSnapshot{Entries: []providerfoundation.LedgerEntry{
		{LedgerKey: providerfoundation.LedgerKey{RouteFamily: providerfoundation.LedgerUnclassified, Dimension: "rest_core"}, Requests: 9},
	}}
	rows := AttachBudgetObservations(payload, claim.Result, snapshot, false, startedAt.Add(time.Second))
	if len(rows) != 1 {
		t.Fatalf("the claim's result gave no comparison (claim result %#v)", claim.Result)
	}
	if err := repository.Complete(ctx, claim, payload, nil, startedAt, startedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := pool.QueryRow(ctx, `SELECT result::text FROM public.sync_run_units WHERE id = $1::uuid`, firstUnitID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var stored struct {
		Observations struct {
			BudgetEstimate []struct {
				RouteFamily    string `json:"route_family"`
				EstimatedUnits int64  `json:"estimated_units"`
			} `json:"budget_estimate"`
			BudgetComparison []BudgetComparisonRow `json:"budget_comparison"`
			AttemptScope     string                `json:"budget_comparison_attempt_scope"`
		} `json:"observations"`
	}
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatalf("stored result %q: %v", raw, err)
	}
	if len(stored.Observations.BudgetEstimate) != 1 || stored.Observations.BudgetEstimate[0].EstimatedUnits != 7 {
		t.Fatalf("stored result = %s, want the budget estimates kept", raw)
	}
	comparison := stored.Observations.BudgetComparison
	if len(comparison) != 1 || comparison[0].RouteFamily != "commits" || comparison[0].ActualRequests != 9 ||
		!comparison[0].Underestimated || stored.Observations.AttemptScope != "final_attempt" {
		t.Fatalf("stored result = %s, want the comparison commits/rest_core 9 against 7", raw)
	}
}
