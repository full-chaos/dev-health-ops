package providersync

import (
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// RequestLedgerSnapshot is the wire requests one complete-route execution made,
// per (route family, dimension), as the unit's request ledger counted them.
type RequestLedgerSnapshot struct {
	Entries []providerfoundation.LedgerEntry
	Dropped int
}

// BudgetEstimateRow is one estimate the dispatch pass admitted the unit on
// (result.observations.budget_estimate).
type BudgetEstimateRow struct {
	RouteFamily    string
	Dimension      string
	EstimatedUnits int64
	Confidence     string
}

// BudgetComparisonRow is one (route family, dimension) of a unit's
// estimate-against-actual comparison (result.observations.budget_comparison),
// the fields Python's _join_budget_estimates_with_actuals reported except the
// bucket identity, which the estimates do not keep.
type BudgetComparisonRow struct {
	RouteFamily                     string   `json:"route_family"`
	Dimension                       string   `json:"dimension"`
	EstimatedUnits                  int64    `json:"estimated_units"`
	ActualRequests                  int64    `json:"actual_requests"`
	Ratio                           *float64 `json:"ratio"`
	Underestimated                  bool     `json:"underestimated"`
	UnderestimationAssessable       bool     `json:"underestimation_assessable"`
	UnderestimationAssessableReason *string  `json:"underestimation_assessable_reason"`
	UnbudgetedActual                bool     `json:"unbudgeted_actual"`
	Incomplete                      bool     `json:"incomplete"`
	FamilyResolved                  bool     `json:"family_resolved"`
}

// BudgetComparisonAttemptScope names what the actuals cover: the execution that
// finished the unit (Python compared the same, its one run of the unit).
const BudgetComparisonAttemptScope = "final_attempt"

// requestCountComparableDimensions are the dimensions whose estimated units
// approximate a request count; the others (graphql cost points, blob
// expansion, the abuse-risk flag) are abstract units a request count cannot be
// compared with.
var requestCountComparableDimensions = map[string]bool{"rest_core": true, "search": true}

// BudgetEstimatesFromResult reads the estimates a unit result carries in
// observations.budget_estimate, from a decoded JSON result or one built in
// memory. Anything else yields none.
func BudgetEstimatesFromResult(result map[string]any) []BudgetEstimateRow {
	observations, _ := result["observations"].(map[string]any)
	raw, _ := observations["budget_estimate"].([]any)
	var estimates []BudgetEstimateRow
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		family, _ := entry["route_family"].(string)
		dimension, _ := entry["dimension"].(string)
		if family == "" || dimension == "" {
			continue
		}
		confidence, _ := entry["confidence"].(string)
		estimates = append(estimates, BudgetEstimateRow{
			RouteFamily: family, Dimension: dimension,
			EstimatedUnits: wholeNumber(entry["estimated_units"]), Confidence: confidence,
		})
	}
	return estimates
}

func wholeNumber(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int:
		return int64(typed)
	case int64:
		return typed
	case json.Number:
		parsed, _ := typed.Int64()
		return parsed
	}
	return 0
}

// CompareBudget joins the unit's estimates with the requests its execution
// made, per (route family, dimension), as Python's comparison did. A labelled
// request is compared under its own family. An unlabelled one takes the one
// estimated family of its dimension that has no labelled requests; with several
// such families it is one joined row ("a+b", family_resolved false). Requests
// with no estimate at all are unbudgeted. An estimate with no requests yields no
// row. earlierWork marks a unit whose earlier attempts or chunks made requests
// this execution did not count.
func CompareBudget(estimates []BudgetEstimateRow, snapshot RequestLedgerSnapshot, earlierWork bool) []BudgetComparisonRow {
	return nil
}

// AttachBudgetObservations puts the unit's budget observations on the result
// payload the success finish stores. The finish replaces the unit's result, so
// the estimates the dispatch pass wrote (carried from claimResult) are put back,
// and the comparison with this execution's requests is added. It returns the
// comparison rows. A claim that carries no estimates leaves the payload alone.
func AttachBudgetObservations(
	payload, claimResult map[string]any, snapshot RequestLedgerSnapshot, earlierWork bool, computedAt time.Time,
) []BudgetComparisonRow {
	return nil
}
