package providersync

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func ledgerOf(entries ...providerfoundation.LedgerEntry) RequestLedgerSnapshot {
	return RequestLedgerSnapshot{Entries: entries}
}

func requests(family, dimension string, count int) providerfoundation.LedgerEntry {
	return providerfoundation.LedgerEntry{LedgerKey: providerfoundation.LedgerKey{RouteFamily: family, Dimension: dimension}, Requests: count}
}

func estimate(family, dimension string, units int64) BudgetEstimateRow {
	return BudgetEstimateRow{RouteFamily: family, Dimension: dimension, EstimatedUnits: units, Confidence: "high"}
}

func rowOf(t *testing.T, rows []BudgetComparisonRow, family, dimension string) BudgetComparisonRow {
	t.Helper()
	for _, row := range rows {
		if row.RouteFamily == family && row.Dimension == dimension {
			return row
		}
	}
	t.Fatalf("no row %s/%s in %+v", family, dimension, rows)
	return BudgetComparisonRow{}
}

func TestBudgetComparisonResolvesUnlabelledRequestsToTheOneFamilyOfTheirDimension(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("commits", "rest_core", 7)},
		ledgerOf(requests(providerfoundation.LedgerUnclassified, "rest_core", 9)), false)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	row := rows[0]
	if row.RouteFamily != "commits" || row.EstimatedUnits != 7 || row.ActualRequests != 9 || !row.FamilyResolved ||
		!row.Underestimated || !row.UnderestimationAssessable || row.UnbudgetedActual || row.Incomplete ||
		row.Ratio == nil || *row.Ratio < 1.28 || *row.Ratio > 1.29 {
		t.Fatalf("row = %+v, want commits/rest_core 9 requests against 7 estimated, underestimated, resolved", row)
	}
}

func TestBudgetComparisonKeepsLabelledFamiliesApartOnOneDimension(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("merge_requests", "rest_core", 10), estimate("notes", "rest_core", 20)},
		ledgerOf(requests("merge_requests", "rest_core", 4), requests("notes", "rest_core", 25)), false)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want two", rows)
	}
	if mr := rowOf(t, rows, "merge_requests", "rest_core"); mr.Underestimated || mr.ActualRequests != 4 || mr.EstimatedUnits != 10 {
		t.Fatalf("merge_requests = %+v", mr)
	}
	if notes := rowOf(t, rows, "notes", "rest_core"); !notes.Underestimated || notes.ActualRequests != 25 || notes.EstimatedUnits != 20 {
		t.Fatalf("notes = %+v", notes)
	}
}

func TestBudgetComparisonJoinsUnlabelledRequestsOfAnAmbiguousDimension(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("merge_requests", "rest_core", 10), estimate("notes", "rest_core", 20)},
		ledgerOf(requests(providerfoundation.LedgerUnclassified, "rest_core", 30)), false)
	if len(rows) != 1 || rows[0].RouteFamily != "merge_requests+notes" || rows[0].FamilyResolved ||
		rows[0].EstimatedUnits != 30 || rows[0].ActualRequests != 30 || rows[0].Underestimated {
		t.Fatalf("rows = %+v, want one joined merge_requests+notes row, 30 against 30, not resolved", rows)
	}
}

func TestBudgetComparisonGivesUnlabelledRequestsTheFamilyNoLabelledRequestsTook(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("merge_requests", "rest_core", 10), estimate("notes", "rest_core", 20)},
		ledgerOf(requests("merge_requests", "rest_core", 4), requests(providerfoundation.LedgerUnclassified, "rest_core", 3)), false)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want two", rows)
	}
	if notes := rowOf(t, rows, "notes", "rest_core"); notes.ActualRequests != 3 || notes.EstimatedUnits != 20 || !notes.FamilyResolved {
		t.Fatalf("notes = %+v, want the 3 unlabelled requests resolved to the one family left", notes)
	}
}

func TestBudgetComparisonReportsRequestsNoEstimateCoversAsUnbudgeted(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("commits", "rest_core", 7)},
		ledgerOf(requests(providerfoundation.LedgerUnclassified, "search", 3)), false)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	row := rows[0]
	if row.RouteFamily != providerfoundation.LedgerUnclassified || row.EstimatedUnits != 0 || row.ActualRequests != 3 ||
		!row.UnbudgetedActual || !row.Underestimated || !row.UnderestimationAssessable || row.Ratio != nil {
		t.Fatalf("row = %+v, want unclassified/search unbudgeted and underestimated with no ratio", row)
	}
}

func TestBudgetComparisonDoesNotCompareRequestsWithAbstractUnits(t *testing.T) {
	t.Parallel()
	rows := CompareBudget(
		[]BudgetEstimateRow{estimate("pr_social", "graphql_cost", 40)},
		ledgerOf(requests(providerfoundation.LedgerUnclassified, "graphql_cost", 100)), false)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	row := rows[0]
	if row.RouteFamily != "pr_social" || row.ActualRequests != 100 || row.UnderestimationAssessable || row.Underestimated ||
		row.Ratio != nil || row.UnderestimationAssessableReason == nil {
		t.Fatalf("row = %+v, want pr_social/graphql_cost reported and not assessable, with its reason", row)
	}
}

func TestBudgetComparisonIsIncompleteWhenRequestsWereDroppedOrEarlierWorkIsNotCounted(t *testing.T) {
	t.Parallel()
	estimates := []BudgetEstimateRow{estimate("commits", "rest_core", 7)}
	entries := requests("commits", "rest_core", 2)
	for name, rows := range map[string][]BudgetComparisonRow{
		"dropped":      CompareBudget(estimates, RequestLedgerSnapshot{Entries: []providerfoundation.LedgerEntry{entries}, Dropped: 3}, false),
		"earlier work": CompareBudget(estimates, ledgerOf(entries), true),
	} {
		if len(rows) != 1 || !rows[0].Incomplete {
			t.Fatalf("%s: rows = %+v, want one incomplete row", name, rows)
		}
	}
	if rows := CompareBudget(estimates, ledgerOf(entries), false); len(rows) != 1 || rows[0].Incomplete {
		t.Fatalf("rows = %+v, want one complete row", rows)
	}
}

func TestBudgetComparisonHasNoRowsWithoutEstimatesOrRequests(t *testing.T) {
	t.Parallel()
	if rows := CompareBudget(nil, ledgerOf(requests("commits", "rest_core", 2)), false); len(rows) != 0 {
		t.Fatalf("rows = %+v without estimates", rows)
	}
	if rows := CompareBudget([]BudgetEstimateRow{estimate("commits", "rest_core", 7)}, RequestLedgerSnapshot{}, false); len(rows) != 0 {
		t.Fatalf("rows = %+v without requests", rows)
	}
}

// The unit result the dispatch pass wrote comes back from the database decoded:
// the estimates are generic JSON values.
func decodedClaimResult(t *testing.T) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal([]byte(`{"keep":"me","observations":{"other":1,"budget_estimate":[`+
		`{"route_family":"commits","dimension":"rest_core","estimated_units":7,"confidence":"high"},`+
		`{"route_family":"pr_social","dimension":"graphql_cost","estimated_units":40,"confidence":"low"}]}}`), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTheFinishCarriesTheEstimatesBackAndAddsTheComparison(t *testing.T) {
	t.Parallel()
	claimResult := decodedClaimResult(t)
	payload := map[string]any{"records": 3, "observations": map[string]any{"route": "kept"}}
	computedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	rows := AttachBudgetObservations(payload,
		claimResult, ledgerOf(requests(providerfoundation.LedgerUnclassified, "rest_core", 9)), false, computedAt)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one", rows)
	}
	observations, ok := payload["observations"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %+v, no observations", payload)
	}
	if observations["route"] != "kept" || payload["records"] != 3 {
		t.Fatalf("the payload's own keys were not kept: %+v", payload)
	}
	if !reflect.DeepEqual(observations["budget_estimate"], claimResult["observations"].(map[string]any)["budget_estimate"]) {
		t.Fatalf("budget_estimate = %+v, want the estimates of the claimed unit", observations["budget_estimate"])
	}
	if comparison, _ := observations["budget_comparison"].([]BudgetComparisonRow); len(comparison) != 1 {
		t.Fatalf("budget_comparison = %+v", observations["budget_comparison"])
	}
	if observations["budget_comparison_computed_at"] != "2026-10-18T12:00:00Z" || observations["budget_comparison_attempt_scope"] != "final_attempt" {
		t.Fatalf("observations = %+v", observations)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	row := stored["observations"].(map[string]any)["budget_comparison"].([]any)[0].(map[string]any)
	if row["route_family"] != "commits" || row["actual_requests"] != float64(9) || row["underestimated"] != true {
		t.Fatalf("stored row = %+v", row)
	}
}

func TestTheFinishCarriesTheEstimatesEvenWhenTheUnitMadeNoCountedRequest(t *testing.T) {
	t.Parallel()
	payload := map[string]any{"records": 3}
	if rows := AttachBudgetObservations(payload, decodedClaimResult(t), RequestLedgerSnapshot{}, false, time.Now()); len(rows) != 0 {
		t.Fatalf("rows = %+v", rows)
	}
	observations, _ := payload["observations"].(map[string]any)
	if observations["budget_estimate"] == nil || observations["budget_comparison"] != nil {
		t.Fatalf("observations = %+v, want the estimates and no comparison", observations)
	}
}

func TestTheFinishLeavesAPayloadAloneWhenTheClaimCarriesNoEstimates(t *testing.T) {
	t.Parallel()
	payload := map[string]any{"records": 3}
	AttachBudgetObservations(payload, map[string]any{"keep": "me"}, ledgerOf(requests("commits", "rest_core", 2)), false, time.Now())
	if _, has := payload["observations"]; has || len(payload) != 1 {
		t.Fatalf("payload = %+v, want it untouched", payload)
	}
}
