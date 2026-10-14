package investmentexplain

import (
	"errors"
	"fmt"
)

// InvestmentShareMeasure is THE measure the investment shares of the REST investment endpoint
// (breakdown and sunburst), the Home tile "Investment by theme" and the Operating Review investment
// section are read in (CHAOS-9622, D5999): the Investment page's COUNT. Lines of code are not a
// measure of effort and Investment offers no lines-of-code option (CHAOS-9695, D6014), so a share is
// never weighted by a unit's effort_value.
//
// It is the ONE place that names the measure. Sizing the work by issue size (CHAOS-9619) changes
// this value and adds a case to MeasureValueSQL; no caller names a measure of its own.
const InvestmentShareMeasure = MeasureCount

// ErrMeasureRequired is returned by a read whose BreakdownFilters name no measure, or one the read
// does not know. There is no default: a read that fell back to a weight would fall back to the
// effort_value that Investment does not read.
var ErrMeasureRequired = errors.New("investmentexplain: the read names no known measure")

// MeasureValueSQL is the SQL of one row's value for a measure: the one definition of COUNT
// for every reader of the work-unit investments (the Investment page's
// analytics.dbExpression/ownershipDbExpression give the same numbers on the same rows).
//
//   - unitWeightSQL is the share of the unit the row stands for: "allocation_weight" under a
//     repository or team scope (one row per unit and repository), "1.0" otherwise.
//   - shareSQL is the share of the repository the teams of the scope hold, "1.0" without a team scope.
//
// COUNT is the unit's probability for the subcategory times those two shares: one unit counts 1
// across the repositories it touches and the subcategories it spreads over.
func MeasureValueSQL(measure, unitWeightSQL, shareSQL string) (string, error) {
	switch measure {
	case MeasureCount:
		return fmt.Sprintf("sum(subcategory_kv.2 * %s * %s)", unitWeightSQL, shareSQL), nil
	}
	return "", fmt.Errorf("%w: %q", ErrMeasureRequired, measure)
}

// requireMeasure refuses a read that names no known measure.
func (f BreakdownFilters) requireMeasure() error {
	switch f.Measure {
	case MeasureCount:
		return nil
	}
	return fmt.Errorf("%w: %q", ErrMeasureRequired, f.Measure)
}
