package analytics

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// BreakdownRequest is the Go port of compiler.py's BreakdownRequest
// dataclass (compiler.py:94-103), non-investment path only -- see
// TimeseriesRequest's doc comment for the scope split.
type BreakdownRequest struct {
	Dimension Dimension
	Measure   Measure
	StartDate graphqldate.Date
	EndDate   graphqldate.Date
	TopN      int
}

// BreakdownRequestFromInput converts the GraphQL input, validating
// dimension/measure/topN like compile_breakdown's validate_* calls plus
// cost.py's validate_top_n (compiler.py:354-356; cost.py:69-90).
func BreakdownRequestFromInput(input model.BreakdownRequestInput) (BreakdownRequest, error) {
	dim, err := dimensionFromInput(input.Dimension)
	if err != nil {
		return BreakdownRequest{}, err
	}
	measure, err := measureFromInput(input.Measure)
	if err != nil {
		return BreakdownRequest{}, err
	}
	if input.DateRange == nil {
		return BreakdownRequest{}, newValidationError("dateRange", nil, "breakdown.dateRange is required")
	}
	if err := validateTopN(input.TopN); err != nil {
		return BreakdownRequest{}, err
	}
	return BreakdownRequest{
		Dimension: dim,
		Measure:   measure,
		StartDate: input.DateRange.StartDate,
		EndDate:   input.DateRange.EndDate,
		TopN:      input.TopN,
	}, nil
}

// validateTopN ports cost.py's validate_top_n (cost.py:69-90): must be
// positive and within CostLimits.max_top_n (cost.py:25, default 100).
const maxTopN = 100

func validateTopN(topN int) error {
	if topN <= 0 {
		return newValidationError("top_n", topN, "top_n must be positive")
	}
	if topN > maxTopN {
		return newValidationError("top_n", topN, "top_n of %d exceeds limit of %d", topN, maxTopN)
	}
	return nil
}

// CompileBreakdown ports compile_breakdown (compiler.py:345-385) for the
// non-investment path -- same wholesale useInvestment=true rejection as
// CompileTimeseries, same reasoning (see that function's doc comment).
func CompileBreakdown(req BreakdownRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (compiledQuery, error) {
	if useInvestment {
		return compiledQuery{}, fmt.Errorf("analytics: CompileBreakdown: investment path not yet ported (CHAOS-4506 follow-up)")
	}

	dimCol, err := dbColumn(req.Dimension, false)
	if err != nil {
		return compiledQuery{}, err
	}
	measureExpr, err := dbExpression(req.Measure, false, false)
	if err != nil {
		return compiledQuery{}, err
	}
	source, alias, dateFilter := nonInvestmentSourceAndDateFilter(req.Measure)

	fc, err := translateFilters(filters, false, filterColumns{Team: "team_id", Repo: "repo_id", Author: "author_email"})
	if err != nil {
		return compiledQuery{}, err
	}

	sql := fmt.Sprintf(`
SELECT
    %s AS dimension_value,
    %s AS value
FROM %s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY dimension_value
ORDER BY value DESC, dimension_value ASC
LIMIT {top_n:UInt32}
SETTINGS max_execution_time = {timeout:UInt64}
`, dimCol, measureExpr, source, dateFilter, alias, fc.sql)

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: req.StartDate.Time()},
		{Name: "end_date", Value: req.EndDate.Time()},
		{Name: "top_n", Value: req.TopN},
		{Name: "timeout", Value: timeoutSeconds},
	}
	bindings = append(bindings, fc.bindings...)

	return compiledQuery{sql: sql, bindings: bindings}, nil
}

// breakdownRow is one row of the raw breakdown query result, before
// label resolution.
type breakdownRow struct {
	DimensionValue string
	Value          float64
}

// ExecuteBreakdownRaw runs the compiled query and returns the raw rows
// -- label resolution (looksLikeUUID / repo+team display-name lookup,
// _resolve_breakdown_labels / _build_breakdown_item, analytics.py:386-429)
// is a SEPARATE step this package does not yet port (it needs the
// identity-service repo/team display-name resolver, api/services/identity.py,
// which lives outside this package's current scope); ExecuteBreakdown
// below applies the label-less fallback shape (BreakdownItem.Label ==
// nil) rather than inventing a resolver. Wiring in a real label resolver
// is follow-up work, tracked alongside the rest of the top-level
// orchestrator wiring.
func executeBreakdownRaw(ctx context.Context, client QueryClient, q compiledQuery) ([]breakdownRow, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []breakdownRow
	for rows.Next() {
		var dimValue string
		var value float64
		if scanErr := rows.Scan(&dimValue, &value); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		out = append(out, breakdownRow{DimensionValue: dimValue, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// ExecuteBreakdown runs the compiled query and returns a BreakdownResult.
// LABEL RESOLUTION NOT YET PORTED (see executeBreakdownRaw's doc comment)
// -- every item's Label is nil, which is NOT yet parity with Python for
// the TEAM/REPO dimensions (Python's A7/A8 framework resolves those to a
// human display name or an "#abcd1234" unresolved token,
// analytics.py:411-429). THIS IS A KNOWN GAP, not a silent omission:
// flagged so a caller does not treat this as dual-run-ready for
// TEAM/REPO breakdowns until the label resolver lands. Dimensions
// without a UUID identity (WORK_TYPE/THEME/SUBCATEGORY on the
// non-investment path -- work_item_type/investment_area/project_stream
// are already human text) are unaffected: Python's own
// _build_breakdown_item falls through to the raw key unchanged for a
// non-UUID-shaped key (analytics.py:421-423), which is exactly what
// leaving Label nil approximates EXCEPT Python still SETS label to that
// same string while this port leaves it nil -- also a gap, tracked the
// same way.
func ExecuteBreakdown(ctx context.Context, client QueryClient, q compiledQuery, dimensionName, measureName string) (model.BreakdownResult, error) {
	rows, err := executeBreakdownRaw(ctx, client, q)
	if err != nil {
		return model.BreakdownResult{}, err
	}
	items := make([]model.BreakdownItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, model.BreakdownItem{Key: r.DimensionValue, Value: r.Value})
	}
	return model.BreakdownResult{Dimension: dimensionName, Measure: measureName, Items: items}, nil
}
