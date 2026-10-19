package investmentexplain

import (
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-go/readers"
)

// The measure each Investment surface reads its shares in: the ONE place that names them; no caller names a
// measure of its own. Lines of code are not a measure of effort and Investment offers no lines-of-code option
// (CHAOS-9695, D6014), so a share is never weighted by a unit's effort_value.
//
// ISSUE_SIZE (D5999) weighs each unit by its issue size, 1 when unsized, and is served only by a surface that
// carries its sizing coverage in the same answer (D6027): the REST investment response (sizing_coverage,
// unsized_unit_count, sizing_note) and the explanation (on each finding). The surfaces that carry no coverage yet
// stay on COUNT (each unit 1) unless asked: the sunburst (REST, by ?measure=ISSUE_SIZE, which then carries its coverage in
// response headers) and the Operating Review (a GraphQL document that selects the section's sizing fields, CHAOS-9873). The Home tile is on COUNT
// (HomeTileMeasure: the REST Home body and a GraphQL Home document that does not ask for the tile's measure and
// coverage) and on ISSUE_SIZE (HomeTileSizedMeasure) only for a GraphQL Home document that selects measure,
// sizingCoverage, unsizedUnitCount and workUnitCount on reworkThemeAllocation (CHAOS-9624).
const (
	RESTInvestmentMeasure     = MeasureIssueSize
	ExplanationDefaultMeasure = MeasureIssueSize
	HomeTileMeasure           = MeasureCount
	HomeTileSizedMeasure      = MeasureIssueSize
	SunburstMeasure           = MeasureCount
	OperatingReviewMeasure    = MeasureCount
	// SunburstSizedMeasure and OperatingReviewSizedMeasure are the opt-in measures of the two surfaces (CHAOS-9873, D6032):
	// each is served only together with the sizing coverage that labels it (D6027).
	SunburstSizedMeasure        = MeasureIssueSize
	OperatingReviewSizedMeasure = MeasureIssueSize
)

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
//
// ISSUE_SIZE (CHAOS-9619, D5999) is COUNT weighed by the unit's issue size, 1 when the unit has none: the source must
// carry readers.UnitSizeWeightColumn (MeasureSource). With no sized unit in scope it equals COUNT.
func MeasureValueSQL(measure, unitWeightSQL, shareSQL string) (string, error) {
	switch measure {
	case MeasureCount:
		return fmt.Sprintf("sum(subcategory_kv.2 * %s * %s)", unitWeightSQL, shareSQL), nil
	case MeasureIssueSize:
		return fmt.Sprintf("sum(subcategory_kv.2 * %s * %s * %s)", unitWeightSQL, shareSQL, readers.UnitSizeWeightColumn), nil
	}
	return "", fmt.Errorf("%w: %q", ErrMeasureRequired, measure)
}

// requireMeasure refuses a read that names no known measure.
func (f BreakdownFilters) requireMeasure() error {
	switch f.Measure {
	case MeasureCount, MeasureIssueSize:
		return nil
	}
	return fmt.Errorf("%w: %q", ErrMeasureRequired, f.Measure)
}

// MeasureSource is the work-unit source ("(...) AS work_unit_investments") a read of measure runs on: for
// ISSUE_SIZE the source with each unit's size weight (readers.IssueSizedSource), for COUNT the source unchanged,
// so a COUNT read's SQL does not move.
func MeasureSource(measure, aliasedSource string) (string, error) {
	if measure != MeasureIssueSize {
		return aliasedSource, nil
	}
	return readers.IssueSizedSource(aliasedSource, "work_unit_investments")
}
