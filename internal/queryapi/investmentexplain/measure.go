package investmentexplain

import (
	"errors"
	"fmt"
)

// InvestmentShareMeasure is THE measure the investment shares of the REST investment endpoint
// (breakdown and sunburst), the Home tile "Investment by theme" and the Operating Review investment
// section are read in (CHAOS-9622, D5999): the Investment page's COUNT. Lines of code are not a
// measure of effort, so a share is never weighted by a unit's effort_value (churn_loc on real data).
//
// It is the ONE place that names the measure. Sizing the work by issue size (CHAOS-9619) changes
// this value and adds a case to MeasureValueSQL; no caller names a measure of its own.
const InvestmentShareMeasure = MeasureCount

// ErrMeasureRequired is returned by a read whose BreakdownFilters name no measure, or one the read
// does not know. There is no default: a read that fell back to a weight would fall back to the
// churn_loc effort_value this ticket removes.
var ErrMeasureRequired = errors.New("investmentexplain: the read names no known measure")

// MeasureValueSQL is the SQL of one row's value for a measure: the one definition of COUNT and
// CHURN_LOC for every reader of the work-unit investments (the Investment page's
// analytics.dbExpression/ownershipDbExpression give the same numbers on the same rows).
//
//   - unitWeightSQL is the share of the unit the row stands for: "allocation_weight" under a
//     repository or team scope (one row per unit and repository), "1.0" otherwise.
//   - shareSQL is the share of the repository the teams of the scope hold, "1.0" without a team scope.
//
// COUNT is the unit's probability for the subcategory times those two shares: one unit counts 1
// across the repositories it touches and the subcategories it spreads over. CHURN_LOC is the changed
// lines of code, and only when asked for by name.
func MeasureValueSQL(measure, unitWeightSQL, shareSQL string) (string, error) {
	switch measure {
	case MeasureCount:
		return fmt.Sprintf("sum(subcategory_kv.2 * %s * %s)", unitWeightSQL, shareSQL), nil
	case MeasureChurnLOC:
		return fmt.Sprintf("sum(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value, 0) * %s)", shareSQL), nil
	}
	return "", fmt.Errorf("%w: %q", ErrMeasureRequired, measure)
}

// requireMeasure refuses a read that names no known measure.
func (f BreakdownFilters) requireMeasure() error {
	switch f.Measure {
	case MeasureCount, MeasureChurnLOC:
		return nil
	}
	return fmt.Errorf("%w: %q", ErrMeasureRequired, f.Measure)
}
