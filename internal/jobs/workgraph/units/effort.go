package units

import "github.com/full-chaos/dev-health-ops/internal/pythonparity"

// Effort metric names, from work_graph/investment/materialize.py.
const (
	EffortMetricChurnLOC    = "churn_loc"
	EffortMetricActiveHours = "active_hours"
)

// Effort is the (metric, value) pair _effort_from_work_unit returns.
type Effort struct {
	Metric string
	Value  float64
}

// EffortInput carries what _effort_from_work_unit reads.
//
// The id slices are used as-is, INCLUDING duplicates: Python sums over the
// iterable, so a repeated id contributes its churn twice. Deduplicating here
// would be the obvious tidy-up and would halve the effort of any unit whose
// component listed a commit more than once.
type EffortInput struct {
	IssueIDs    []string
	PRIDs       []string
	CommitIDs   []string
	PRChurn     map[string]float64
	CommitChurn map[string]float64
	ActiveHours map[string]float64
}

// ComputeEffort ports materialize._effort_from_work_unit.
//
// A strict priority chain -- commit churn, then PR churn, then active hours,
// then a zero -- where each tier is taken only if its total is STRICTLY
// POSITIVE.
//
// # THE `> 0` GATE IS A FALL-THROUGH, NOT A VALIDITY CHECK
//
// That distinction is the whole behaviour, and three cases show why writing it
// as `!= 0` or as "if the tier has data" would diverge:
//
//   - a NEGATIVE total falls through to the next tier. Measured: commit churn
//     of -5 with PR churn of 20 yields ("churn_loc", 20) from the PR tier, not
//     the commit tier and not -5.
//   - a NaN total falls through too, because `nan > 0` is false in both
//     languages. Measured: NaN commit churn with PR churn 7 yields 7. This one
//     is free -- Go's comparison agrees with Python's -- but only because
//     neither language propagates NaN through a `>`.
//   - ALL tiers non-positive yields ("churn_loc", 0.0), NOT the negative sum
//     and NOT the active-hours metric. The final fallback names churn_loc even
//     when the unit had no commits or PRs at all.
//
// Infinity is returned unchanged: `inf > 0` is true, so it is selected and
// passed through as the effort value.
func ComputeEffort(input EffortInput) Effort {
	if total := sumChurn(input.CommitIDs, input.CommitChurn); total > 0 {
		return Effort{Metric: EffortMetricChurnLOC, Value: total}
	}
	if total := sumChurn(input.PRIDs, input.PRChurn); total > 0 {
		return Effort{Metric: EffortMetricChurnLOC, Value: total}
	}
	if total := sumChurn(input.IssueIDs, input.ActiveHours); total > 0 {
		return Effort{Metric: EffortMetricActiveHours, Value: total}
	}
	return Effort{Metric: EffortMetricChurnLOC, Value: 0.0}
}

// sumChurn is Python's `sum(table.get(id, 0.0) for id in ids)`.
//
// Iteration follows the ID slice, not the map, for two reasons that both
// matter: a missing id contributes 0.0 rather than being skipped, and the
// addition order is the caller's order. Summing the MAP instead would drop
// duplicates and reorder the additions, and floating-point addition is not
// associative -- two orders can differ in the last bit, which then decides a
// `> 0` comparison for a total that sums to near zero.
func sumChurn(ids []string, table map[string]float64) float64 {
	values := make([]float64, len(ids))
	for index, id := range ids {
		// Python's .get(id, 0.0): absent is zero, not a skip. Go's zero value
		// for a missing key is already 0.0, so the lookup needs no guard.
		values[index] = table[id]
	}
	// pythonparity.Sum, NOT a `total +=` loop. CPython's sum() has used
	// Neumaier compensated summation for floats since 3.12, and the two differ
	// on roughly two in five multi-value inputs. Here the total is then
	// compared against zero to pick a tier, so a last-bit difference can select
	// a DIFFERENT effort metric for a churn list that cancels to near zero.
	return pythonparity.Sum(values)
}
