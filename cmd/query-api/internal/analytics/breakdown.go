package analytics

import (
	"context"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/scopelabel"

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

// CompileBreakdown ports compile_breakdown (compiler.py:345-385,
// e9ea257ff) for BOTH the non-investment and investment (CHAOS-4538)
// paths -- same investmentContextFor wiring as CompileTimeseries, see
// that function's doc comment.
func CompileBreakdown(req BreakdownRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (compiledQuery, error) {
	dimCol, err := dbColumn(req.Dimension, useInvestment)
	if err != nil {
		return compiledQuery{}, err
	}

	fc, err := translateFilters(filters, useInvestment, defaultFilterColumns())
	if err != nil {
		return compiledQuery{}, err
	}

	var source, alias, dateFilter, extraClauses, measureExpr string
	if useInvestment {
		ictx := investmentContextFor([]Dimension{req.Dimension}, needsTeamJoin(filters), needsAuthorJoin(filters))
		source, alias, dateFilter, extraClauses = ictx.Source, ictx.Alias, ictx.DateFilter, ictx.ExtraClauses
		measureExpr, err = dbExpression(req.Measure, true, ictx.UseRepoAllocation)
		if err != nil {
			return compiledQuery{}, err
		}
	} else {
		source, alias, dateFilter = nonInvestmentSourceAndDateFilter(req.Measure)
		measureExpr, err = dbExpression(req.Measure, false, false)
		if err != nil {
			return compiledQuery{}, err
		}
	}
	// Force a uniform Float64 result type. ClickHouse returns UInt64 for
	// the SUM()-based measures (COUNT, THROUGHPUT, CHURN_LOC) and Float64
	// for the AVG/ratio ones, and the native driver will NOT convert
	// between them at scan time -- it errors, exactly as
	// reviewedges.go:145 documents for UInt32. Coercing in SQL keeps ONE
	// scan type for every measure. Python does the same coercion one
	// layer later with `float(row["value"])`, so values are unchanged.
	measureExpr = "toFloat64(" + measureExpr + ")"

	sql := fmt.Sprintf(`
SELECT
    %s AS dimension_value,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY dimension_value
ORDER BY value DESC, dimension_value ASC
LIMIT {top_n:UInt32}
%s
`, dimCol, measureExpr, source, extraClauses, dateFilter, alias, fc.sql, settingsMaxExecutionTime(timeoutSeconds))

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "top_n", Value: req.TopN},
	}
	bindings = append(bindings, fc.bindings...)

	return compiledQuery{sql: sql, bindings: bindings}, nil
}

// breakdownRow is one row of the raw breakdown query result, before
// label resolution.
//
// Value is *float64, not float64 -- CHAOS-4650 (chris 2026-08-31 04:18,
// Option B). dbExpression's category-2 measures (validate.go's AT-RISK
// Nullable(Float64) list) can return SQL NULL for an all-NULL group even
// after the toFloat64(...) coercion in CompileBreakdown -- Nullable
// propagates through toFloat64, it does not collapse it. Scanning that
// NULL into a bare, non-pointer float64 silently reads back as the Go
// zero value 0.0 (verified against the pinned clickhouse-go v2.47.0
// driver: Float64.ScanRow's `case *float64` branch never sees the NULL
// at all -- Nullable.ScanRow intercepts it first and only recognises
// `case **float64: *v = nil` as a nullable-aware destination), which is
// indistinguishable on the wire from "genuinely measured zero". That
// silent collapse is the defect this ticket removes: scanning into
// *float64 (Nullable.ScanRow's own documented nullable destination,
// clickhouse-go/v2/lib/column/nullable.go) preserves the NULL as a nil
// pointer instead of manufacturing a number, and BreakdownItem.Value
// carries it through so a nil marshals as a genuine JSON null rather
// than the literal 0.
//
// EXPECTED DIVERGENCE, class = product-decision (CHAOS-4650, chris
// 2026-08-31 04:18): Python's _build_breakdown_item
// (analytics.py:367, `float(row.get("value") or 0)`) still collapses
// the same all-NULL case to 0.0 -- deliberately left unchanged (root
// AGENTS.md GO-ONLY rule: no further work in the Python GraphQL layer).
// This is a one-field divergence, not a prefix: it names exactly
// BreakdownItem.Value on the non-investment breakdown path and no
// other query. Do not resolve it by reverting Go to Python's 0.0
// collapse, and do not read it as covering any other measure/field
// pair sharing the AVG(Nullable(Float64)) shape. timeseries.go's
// ExecuteTimeseries scanned the same category-2 measures into a bare
// float64 at the same shape's defect -- CHAOS-4657 fixed that path with
// the identical *float64 scan and the same product ruling; see
// timeseries.go's ExecuteTimeseries doc comment.
//
// REACHABILITY, checked by mechanism not by ticket state (CHAOS-4538
// itself merged 08-30/31, bba15566d -- citing it as a blocker is
// stale): `investmentBreakdown` IS pre-registered in query_route.go,
// but dispatch requires its OWN row in routeswitch's dynamic switch
// (switch.go), and no go_api_routing_state row currently enables
// `investmentBreakdown` or `investmentFull` -- verified locally.
// PRODUCTION ROUTING STATE IS UNVERIFIED; this is a local fact only.
// So nothing observes the internal/schema nullability mismatch TODAY
// -- BUT THAT COULD CHANGE THE MOMENT A ROUTING-STATE ROW IS ADDED,
// WITH NO FURTHER CODE CHANGE. That is the sharp edge of CHAOS-4658:
// the mismatch becomes observable through a DATA change (a routing
// row), not a code change, so no code review will ever catch it
// turning live. Whoever enables that row MUST widen
// contracts/graphql/v1/schema.graphql's `value: Float!` to `value:
// Float` (and its Python Strawberry counterpart,
// src/dev_health_ops/api/graphql/models/outputs.py's BreakdownItem)
// in that same change, or a live all-NULL group will make gqlgen's
// exec engine reject the whole response ("must not be null") instead
// of rendering the empty state this ticket exists to enable. THIS
// COMMENT, NOT the ones in models_gen.go/generated.go, is the durable
// copy: gqlgen generate overwrites both of those files wholesale (see
// this repo's PR history/CI logs for the CHAOS-4650 PR for an
// executed proof that a regen reverts BreakdownItem.Value to float64
// and fails the build at this file's own construction site) -- this
// file is hand-written and survives regeneration, so the explanation
// of why lives here, not there.
type breakdownRow struct {
	DimensionValue string
	Value          *float64
}

// executeBreakdownRaw runs the compiled query and returns the raw rows;
// ExecuteBreakdown adds the item labels.
func executeBreakdownRaw(ctx context.Context, client QueryClient, q compiledQuery) ([]breakdownRow, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []breakdownRow
	for rows.Next() {
		var dimValue string
		var value *float64 // nullable scan destination -- see breakdownRow.Value's doc comment (CHAOS-4650)
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
//
// Every item carries the server-resolved label the client renders instead of
// a raw id: for the repo and team dimensions the key is resolved to its
// display name through the shared lookup (internal/scopelabel); a resolved
// name that is a bare UUID is dropped; any key that is not a bare UUID
// labels itself; a bare-UUID key becomes a short "#" token (its first eight
// hex digits) so the client shows an unresolved badge, never a raw id. The
// lookup is best effort: a failure leaves every item on the fallback path.
// The tables it reads are read as stored, one row per physical version, the
// way the Python resolver reads them.
func ExecuteBreakdown(ctx context.Context, client QueryClient, orgID string, q compiledQuery, dimensionName, measureName string) (model.BreakdownResult, error) {
	rows, err := executeBreakdownRaw(ctx, client, q)
	if err != nil {
		return model.BreakdownResult{}, err
	}
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, r.DimensionValue)
	}
	labels := breakdownLabels(ctx, client, orgID, dimensionName, keys)
	items := make([]model.BreakdownItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, model.BreakdownItem{Key: r.DimensionValue, Value: r.Value, Label: breakdownLabel(r.DimensionValue, labels)})
	}
	return model.BreakdownResult{Dimension: dimensionName, Measure: measureName, Items: items}, nil
}

// breakdownLabels resolves the keys of a repo or team breakdown to display
// names; every other dimension is already human text and resolves nothing.
func breakdownLabels(ctx context.Context, client QueryClient, orgID, dimensionName string, keys []string) map[string]string {
	kind := strings.ToLower(dimensionName)
	if kind != "repo" && kind != "team" {
		return map[string]string{}
	}
	var q scopelabel.Querier
	if client != nil {
		q = client
	}
	return scopelabel.Resolve(ctx, q, orgID, kind, keys, scopelabel.Options{Log: "analytics"})
}

// breakdownLabel is the label of one breakdown item.
func breakdownLabel(key string, labels map[string]string) *string {
	if resolved := labels[key]; resolved != "" && !scopelabel.LooksLikeUUID(resolved) {
		return &resolved
	}
	if key != "" && !scopelabel.LooksLikeUUID(key) {
		return &key
	}
	token := []rune(strings.ReplaceAll(key, "-", ""))
	if len(token) > 8 {
		token = token[:8]
	}
	if len(token) == 0 {
		return nil
	}
	label := "#" + string(token)
	return &label
}
