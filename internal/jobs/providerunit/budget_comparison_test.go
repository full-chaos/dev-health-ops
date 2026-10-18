package providerunit

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func ratio(value float64) *float64 { return &value }

// A finished unit logs its comparison once, warns and counts only for a row
// whose requests exceeded a request-count estimate, and says nothing for a
// unit whose rows are all within their estimates or on abstract-unit
// dimensions. Not parallel: it swaps the default logger.
func TestObserveBudgetComparisonWarnsAndCountsOnlyForAssessableUnderestimates(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	claim := providersync.Claim{}
	claim.Provider, claim.Dataset, claim.ID = "github", "commits", "22222222-2222-4222-8222-222222222222"
	reason := "abstract unit"
	rows := []providersync.BudgetComparisonRow{
		{RouteFamily: "commits", Dimension: "rest_core", EstimatedUnits: 7, ActualRequests: 9, Ratio: ratio(9.0 / 7), Underestimated: true, UnderestimationAssessable: true, FamilyResolved: true},
		{RouteFamily: "prs", Dimension: "rest_core", EstimatedUnits: 10, ActualRequests: 4, Ratio: ratio(0.4), UnderestimationAssessable: true, FamilyResolved: true},
		{RouteFamily: "pr_social", Dimension: "graphql_cost", EstimatedUnits: 40, ActualRequests: 100, UnderestimationAssessableReason: &reason, FamilyResolved: true},
		{RouteFamily: providerfoundation.LedgerUnclassified, Dimension: "search", ActualRequests: 3, Underestimated: true, UnderestimationAssessable: true, UnbudgetedActual: true},
	}
	metrics := providerfoundation.NewMetrics()
	(&Handler{ProviderMetrics: metrics}).observeBudgetComparison(context.Background(), claim, rows, 0, false)

	logged := logs.String()
	if got := strings.Count(logged, "msg=sync_provider_unit.budget_comparison "); got != 1 {
		t.Fatalf("comparison lines = %d, want 1 in:\n%s", got, logged)
	}
	if !strings.Contains(logged, "rows=4 assessable=3 underestimated=2") {
		t.Fatalf("comparison line does not carry the counts:\n%s", logged)
	}
	if got := strings.Count(logged, "msg=sync_provider_unit.budget_underestimated "); got != 2 {
		t.Fatalf("underestimate warnings = %d, want 2 (commits, unbudgeted search) in:\n%s", got, logged)
	}
	if !strings.Contains(logged, "reason=unbudgeted_actual") || !strings.Contains(logged, "route_family=commits") ||
		strings.Contains(logged, "route_family=pr_social level=WARN") {
		t.Fatalf("warnings are not the assessable underestimates:\n%s", logged)
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`dev_health_provider_budget_underestimate_total{provider="github",dataset="commits",dimension="rest_core"} 1`,
		`dev_health_provider_budget_underestimate_total{provider="github",dataset="commits",dimension="search"} 1`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in:\n%s", want, output.String())
		}
	}
	if strings.Contains(output.String(), `dimension="graphql_cost"`) {
		t.Fatalf("an abstract-unit row was counted:\n%s", output.String())
	}
}

func TestObserveBudgetComparisonSaysNothingForAUnitWithoutRows(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	(&Handler{ProviderMetrics: providerfoundation.NewMetrics()}).observeBudgetComparison(context.Background(), providersync.Claim{}, nil, 0, false)
	(*Handler)(nil).observeBudgetComparison(context.Background(), providersync.Claim{}, nil, 0, false)
	if logs.Len() != 0 {
		t.Fatalf("logged for a unit without rows:\n%s", logs.String())
	}
}

// The success finish replaces the unit's result, so the estimates the dispatch
// pass wrote are carried back into it and the comparison with the requests the
// execution made is added; an underestimate is counted.
func TestAFinishedUnitStoresItsEstimatesAndTheComparisonWithItsRequests(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Result = map[string]any{"keep": "me", "observations": map[string]any{"budget_estimate": []any{
		map[string]any{"route_family": "feature_flags", "dimension": "rest_core", "estimated_units": float64(1), "confidence": "high"},
	}}}
	repository := newMemoryUnitRepository(unit)
	metrics := providerfoundation.NewMetrics()
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now }, BuildExecutor: successfulExecutor(t, now), ProviderMetrics: metrics,
	}
	if err := handler.Work(context.Background(), providerExecution(repository.unit, now, 1)); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	observations, ok := repository.result["observations"].(map[string]any)
	if !ok {
		t.Fatalf("terminal result = %#v, no observations", repository.result)
	}
	if estimates, _ := observations["budget_estimate"].([]any); len(estimates) != 1 {
		t.Fatalf("budget_estimate = %#v, want the estimate the unit was admitted on", observations["budget_estimate"])
	}
	rows, _ := observations["budget_comparison"].([]providersync.BudgetComparisonRow)
	if len(rows) != 1 || rows[0].RouteFamily != "feature_flags" || rows[0].EstimatedUnits != 1 || rows[0].ActualRequests != 2 || !rows[0].Underestimated {
		t.Fatalf("budget_comparison = %#v, want feature_flags/rest_core 2 requests against 1 estimated", observations["budget_comparison"])
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `dev_health_provider_budget_underestimate_total{provider="launchdarkly",dataset="feature-flags",dimension="rest_core"} 1`) {
		t.Fatalf("the underestimate was not counted:\n%s", output.String())
	}
}

func TestAFinishedUnitWithoutEstimatesStoresNoBudgetObservations(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now }, BuildExecutor: successfulExecutor(t, now), ProviderMetrics: providerfoundation.NewMetrics(),
	}
	if err := handler.Work(context.Background(), providerExecution(repository.unit, now, 1)); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if _, has := repository.result["observations"].(map[string]any)["budget_estimate"]; has {
		t.Fatalf("terminal result = %#v, want no budget observations for a unit without estimates", repository.result)
	}
}
