package providersync

import (
	"encoding/json"
	"sort"
	"strings"
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
// compared with. A row with no estimate is compared on any dimension: a zero
// baseline is no unit conversion (Python's sync_units.py:443-445).
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
	if len(estimates) == 0 || len(snapshot.Entries) == 0 {
		return nil
	}
	type key struct{ family, dimension string }
	estimated := map[key]int64{}
	familiesOf := map[string][]string{}
	for _, estimate := range estimates {
		k := key{estimate.RouteFamily, estimate.Dimension}
		if _, known := estimated[k]; !known {
			familiesOf[estimate.Dimension] = append(familiesOf[estimate.Dimension], estimate.RouteFamily)
		}
		estimated[k] += estimate.EstimatedUnits
	}
	labelled := map[key]int64{}
	unlabelled := map[string]int64{}
	for _, entry := range snapshot.Entries {
		if entry.RouteFamily == providerfoundation.LedgerUnclassified {
			unlabelled[entry.Dimension] += int64(entry.Requests)
			continue
		}
		labelled[key{entry.RouteFamily, entry.Dimension}] += int64(entry.Requests)
	}
	type row struct {
		family, dimension string
		units, actual     int64
		budgeted          bool
		resolved          bool
	}
	var rows []row
	for k, actual := range labelled {
		units, budgeted := estimated[k]
		rows = append(rows, row{k.family, k.dimension, units, actual, budgeted, true})
	}
	for dimension, actual := range unlabelled {
		var open []string
		for _, family := range familiesOf[dimension] {
			if _, taken := labelled[key{family, dimension}]; !taken {
				open = append(open, family)
			}
		}
		sort.Strings(open)
		var units int64
		for _, family := range open {
			units += estimated[key{family, dimension}]
		}
		switch len(open) {
		case 0:
			rows = append(rows, row{providerfoundation.LedgerUnclassified, dimension, 0, actual, false, false})
		case 1:
			rows = append(rows, row{open[0], dimension, units, actual, true, true})
		default:
			rows = append(rows, row{strings.Join(open, "+"), dimension, units, actual, true, false})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].family != rows[j].family {
			return rows[i].family < rows[j].family
		}
		return rows[i].dimension < rows[j].dimension
	})
	incomplete := snapshot.Dropped > 0 || earlierWork
	out := make([]BudgetComparisonRow, 0, len(rows))
	for _, r := range rows {
		unbudgeted := !r.budgeted
		assessable := unbudgeted || requestCountComparableDimensions[r.dimension]
		comparison := BudgetComparisonRow{
			RouteFamily: r.family, Dimension: r.dimension, EstimatedUnits: r.units, ActualRequests: r.actual,
			UnderestimationAssessable: assessable, UnbudgetedActual: unbudgeted,
			Incomplete: incomplete, FamilyResolved: r.resolved,
		}
		if assessable {
			if r.units > 0 {
				ratio := float64(r.actual) / float64(r.units)
				comparison.Ratio = &ratio
			}
			comparison.Underestimated = r.actual > r.units
		} else {
			reason := "'" + r.dimension + "' is an abstract reservation unit, not a 1:1 request count -- comparing actual_requests against it would invent a conversion the estimator never made"
			comparison.UnderestimationAssessableReason = &reason
		}
		out = append(out, comparison)
	}
	return out
}

// AttachBudgetObservations puts the unit's budget observations on the result
// payload the success finish stores. The finish replaces the unit's result, so
// the estimates the dispatch pass wrote (carried from claimResult) are put back,
// and the comparison with this execution's requests is added. It returns the
// comparison rows. A claim that carries no estimates leaves the payload alone.
func AttachBudgetObservations(
	payload, claimResult map[string]any, snapshot RequestLedgerSnapshot, earlierWork bool, computedAt time.Time,
) []BudgetComparisonRow {
	estimates := BudgetEstimatesFromResult(claimResult)
	if len(estimates) == 0 {
		return nil
	}
	claimObservations, _ := claimResult["observations"].(map[string]any)
	observations := map[string]any{}
	if existing, ok := payload["observations"].(map[string]any); ok {
		for name, value := range existing {
			observations[name] = value
		}
	}
	observations["budget_estimate"] = claimObservations["budget_estimate"]
	rows := CompareBudget(estimates, snapshot, earlierWork)
	if len(rows) > 0 {
		observations["budget_comparison"] = rows
		observations["budget_comparison_computed_at"] = computedAt.UTC().Format(time.RFC3339Nano)
		observations["budget_comparison_attempt_scope"] = BudgetComparisonAttemptScope
	}
	payload["observations"] = observations
	return rows
}
