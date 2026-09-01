package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// investmentMetricsDailyDedupSource ports compiler.py's
// _INVESTMENT_METRICS_DAILY_DEDUP (compiler.py:63-79) verbatim -- the
// default non-investment source table for timeseries/breakdown, deduped
// via argMax(col, computed_at) because investment_metrics_daily is a
// plain MergeTree (migration 007) that does not self-merge duplicate
// (re)writes of the same natural key (CHAOS-2710).
const investmentMetricsDailyDedupSource = `(
    SELECT
        org_id,
        day,
        repo_id,
        team_id,
        investment_area,
        project_stream,
        argMax(delivery_units, computed_at) AS delivery_units,
        argMax(work_items_completed, computed_at) AS work_items_completed,
        argMax(prs_merged, computed_at) AS prs_merged,
        argMax(churn_loc, computed_at) AS churn_loc,
        argMax(cycle_p50_hours, computed_at) AS cycle_p50_hours
    FROM investment_metrics_daily
    WHERE org_id = {org_id:String}
    GROUP BY org_id, day, repo_id, team_id, investment_area, project_stream
) AS investment_metrics_daily`

// appendOnlyDailyNaturalKeys is the subset of clickhouse_dedup.py's
// _APPEND_ONLY_DAILY_KEYS registry this package's Measure.source_table()
// range can actually produce -- NOT a general port of the whole
// registry, which has entries this package never reads
// (dora_metrics_daily, file_metrics_daily, etc.). Scoped deliberately;
// extend it only if a new Measure routes to a new source table.
var appendOnlyDailyNaturalKeys = map[string][]string{
	"testops_pipeline_metrics_daily": {"org_id", "repo_id", "day"},
	"testops_test_metrics_daily":     {"org_id", "repo_id", "day"},
	"testops_coverage_metrics_daily": {"org_id", "repo_id", "day"},
	"repo_metrics_daily":             {"org_id", "repo_id", "day"},
	// release_impact_daily deliberately absent -- CHAOS-4536, confirmed
	// unregistered on the Python side too (clickhouse_dedup.py has no
	// entry for it, and it is not in RERUN_DEDUPED_DAILY_TABLES either:
	// plain MergeTree, 034_feature_flag_user_impact_tables.sql:99). This
	// port copies that gap faithfully -- see dedupFromMeasureSource's doc
	// comment. NOT a CHAOS-4506 fix site.
}

// dedupFromMeasureSource ports dedup_from (clickhouse_dedup.py:132-156)
// scoped to exactly the tables Measure.source_table (validate.go) can
// return -- Measure.source_table's tables are never in
// RERUN_DEDUPED_DAILY_TABLES (that set is {work_item_metrics_daily,
// work_item_user_metrics_daily}, neither of which any Measure in this
// package's scope reads), so the ReplacingMergeTree/FINAL branch of the
// real dedup_from never triggers here and is intentionally not
// reproduced. Returns (source SQL, FROM-clause alias).
//
// release_impact_daily returns the RAW table name, unwrapped -- Python's
// dedup_from falls through to `return table` for any table absent from
// both dedup registries, and this table is (CHAOS-4536). This is
// existing, still-live `main` behavior; hand-copied faithfully, not "fixed"
// here (root AGENTS.md's SQL-sourcing ruling: fix Python first, on `main`,
// THEN port -- this has not happened, so the port stays faithful to what
// ships today, undeduped).
func dedupFromMeasureSource(table string) (source string, alias string) {
	key, ok := appendOnlyDailyNaturalKeys[table]
	if !ok {
		return table, table
	}
	return fmt.Sprintf(`(
            SELECT *
            FROM %s
            ORDER BY computed_at DESC
            LIMIT 1 BY %s
        ) AS %s`, table, joinColumns(key), table), table
}

func joinColumns(cols []string) string {
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += c
	}
	return out
}

// nonInvestmentSourceAndDateFilter resolves the FROM source + date
// predicate for a non-investment timeseries/breakdown query, applying
// Measure.source_table's testops/feature-flag override exactly like
// compile_timeseries/compile_breakdown do (compiler.py:325-328,369-372):
// the override REPLACES source_table and date_filter unconditionally
// once a measure routes to a non-default table, regardless of anything
// else about the request.
func nonInvestmentSourceAndDateFilter(measure Measure) (source, alias, dateFilter string) {
	if table := measureSourceTable(measure); table != "" {
		src, al := dedupFromMeasureSource(table)
		return src, al, "day >= {start_date:Date} AND day <= {end_date:Date}"
	}
	return investmentMetricsDailyDedupSource, "investment_metrics_daily", "day >= {start_date:Date} AND day <= {end_date:Date}"
}

// TimeseriesRequest is the Go port of compiler.py's TimeseriesRequest
// dataclass (compiler.py:82-91), restricted to the NON-INVESTMENT path
// (CHAOS-4506 scope split, orchestrator-approved: the investment-CTE
// path is a separate follow-up ticket). A request whose batch-level
// useInvestment resolves true is rejected explicitly by
// CompileTimeseries below -- never silently answered with
// non-investment numbers.
type TimeseriesRequest struct {
	Dimension Dimension
	Measure   Measure
	Interval  BucketInterval
	StartDate graphqldate.Date
	EndDate   graphqldate.Date
}

func intervalFromInput(i model.BucketIntervalInput) (BucketInterval, error) {
	switch i {
	case model.BucketIntervalInputDay:
		return BucketIntervalDay, nil
	case model.BucketIntervalInputWeek:
		return BucketIntervalWeek, nil
	case model.BucketIntervalInputMonth:
		return BucketIntervalMonth, nil
	}
	return "", newValidationError("interval", string(i), "invalid interval: %q", i)
}

// TimeseriesRequestFromInput converts the GraphQL input, validating
// dimension/measure/interval like compile_timeseries's validate_*
// calls (compiler.py:309-311).
func TimeseriesRequestFromInput(input model.TimeseriesRequestInput) (TimeseriesRequest, error) {
	dim, err := dimensionFromInput(input.Dimension)
	if err != nil {
		return TimeseriesRequest{}, err
	}
	measure, err := measureFromInput(input.Measure)
	if err != nil {
		return TimeseriesRequest{}, err
	}
	interval, err := intervalFromInput(input.Interval)
	if err != nil {
		return TimeseriesRequest{}, err
	}
	if input.DateRange == nil {
		return TimeseriesRequest{}, newValidationError("dateRange", nil, "timeseries.dateRange is required")
	}
	return TimeseriesRequest{
		Dimension: dim,
		Measure:   measure,
		Interval:  interval,
		StartDate: input.DateRange.StartDate,
		EndDate:   input.DateRange.EndDate,
	}, nil
}

// firstDateFilterToken ports templates.py's `date_col =
// date_filter.split(" ")[0]` (templates.py:41) exactly -- including
// investment path's slightly odd consequence: its date_filter
// (compiler.py:227) is "work_unit_investments.from_ts < ... AND
// work_unit_investments.to_ts >= ...", so the timeseries bucket column
// becomes `work_unit_investments.from_ts` (the work unit's START
// timestamp), not a generic "day" column -- a real Python behavior this
// port reproduces faithfully rather than "fixing" into something that
// reads more sensibly.
func firstDateFilterToken(dateFilter string) string {
	fields := strings.Fields(dateFilter)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// CompileTimeseries ports compile_timeseries (compiler.py:291-342,
// e9ea257ff) for BOTH the non-investment and investment (CHAOS-4538)
// paths. useInvestment is the resolved batch-level flag (analytics.py:554's
// `bool(batch.use_investment)`) -- BreakdownRequestInput/
// TimeseriesRequestInput carry no per-item override (schema.graphql;
// confirmed against model.TimeseriesRequestInput/BreakdownRequestInput,
// neither has a UseInvestment field), so this is threaded down from the
// batch by the caller, not read off req itself.
func CompileTimeseries(req TimeseriesRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (compiledQuery, error) {
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
		// investment-path queries execute against org 70d529e0's live
		// membership state on every call -- RecordStaleInvestmentMembershipScope
		// is invoked by CompileTimeseries's caller (resolve.go's
		// resolveOneTimeseries) once the compiled query is about to
		// execute; see that call site's doc comment for why compile-time
		// is the wrong place (Python fires the telemetry query from
		// _query_investment_dicts, which wraps EXECUTION, not
		// compilation -- investment.py:175-181).
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

	dateCol := firstDateFilterToken(dateFilter)

	sql := fmt.Sprintf(`
SELECT
    date_trunc('%s', %s) AS bucket,
    %s AS dimension_value,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY bucket, dimension_value
ORDER BY bucket ASC, value DESC, dimension_value ASC
%s
`, dateTruncUnit(req.Interval), dateCol, dimCol, measureExpr, source, extraClauses, dateFilter, alias, fc.sql, settingsMaxExecutionTime(timeoutSeconds))

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
	}
	bindings = append(bindings, fc.bindings...)

	return compiledQuery{sql: sql, bindings: bindings}, nil
}

// ExecuteTimeseries runs the compiled query and groups rows by
// dimension_value into TimeseriesResult, porting
// _execute_timeseries_query's grouping shape (analytics.py:334-383)
// exactly: one TimeseriesResult per distinct dimension_value observed,
// buckets appended in row-arrival order (the SQL's own `ORDER BY bucket
// ASC` already sorts them; Python does not re-sort after grouping,
// neither does this).
//
// The value column is scanned into *float64, not float64 -- CHAOS-4657
// (chris 2026-08-31 04:18, Option B), the SAME shape and SAME product
// ruling CHAOS-4650 applied to breakdown.go's breakdownRow.Value; see
// that doc comment for the full mechanism writeup (verified against
// pinned clickhouse-go v2.47.0: Nullable.ScanRow only recognises **T
// for its NULL branch -- a bare *float64 destination never observes the
// NULL at all and silently keeps its zero-initialised 0.0). CompileTimeseries
// wraps every measure expression in toFloat64(...), which does NOT
// collapse a Nullable(Float64) source -- an all-NULL bucket (a
// dimension_value whose category-2 AT-RISK measure has no rows, or
// whose rows are all NULL, in that date bucket) still reaches the scan
// as SQL NULL. Scanning into *float64 preserves that as a nil pointer,
// and TimeseriesBucket.Value (models_gen.go, hand-edited to *float64
// the same way) carries it through so a nil marshals as a genuine JSON
// null the UI's empty state can render, instead of the literal 0 an
// "avg() over nothing" cannot be told apart from.
//
// EXPECTED DIVERGENCE, class = product-decision (CHAOS-4650 ruling,
// applied here verbatim): Python's timeseries builder collapses the
// same all-NULL case to 0.0 unchanged (root AGENTS.md GO-ONLY rule bars
// touching the Python GraphQL layer for this). Do not resolve this by
// reverting Go to Python's 0.0 collapse.
//
// REACHABILITY / CHAOS-4658 hazard, checked by mechanism, NOT assumed
// identical to breakdown.go's (2026-08-31 lane-4657 sweep): the batch
// `analytics.timeseries` field this function serves is NOT selected by
// EITHER investmentBreakdown or investmentFull (query_route.go's
// registeredInvestmentBreakdownDocument/registeredInvestmentFullDocument
// select only breakdowns/evidenceQuality*/sankey -- confirmed by
// reading both document literals). Its REAL web consumers are four
// DIFFERENT operations -- FeatureFlagTimeseries
// (web/src/lib/feature-flags/queries.ts) and TestOpsPipeline/
// TestOpsTest/TestOpsCoverage (web/src/lib/testops/queries.ts), all of
// which select `analytics.timeseries.buckets.value` and feed it straight
// into unguarded arithmetic (web/src/lib/feature-flags/fetchers.ts's
// mergeToSpark: `values.reduce((s, v) => s + v, 0) / values.length)` --
// no empty-state branch for a null bucket; JS's `+` coerces a null
// operand to 0, so a null would be silently averaged in as a measured
// zero, reproducing THIS SAME defect one layer up in the web client
// were it ever null on the wire). NONE of these four operations appear
// in query_route.go's digestByOperation map at all -- confirmed by grep
// -- so they are not merely gated OFF by a missing go_api_routing_state
// row (breakdown's situation): they have no registered document AT
// ALL, which is a SEPARATE, EARLIER gate than PostgresSwitch.Enabled().
// Registering one of these four documents is therefore its own
// deliberate future step, prior to and independent of any routing-state
// row -- timeseries's blast radius today is narrower than breakdown's,
// not identical to it. Whoever takes that registration step MUST widen
// the SDL's TimeseriesBucket.value (and BreakdownItem.value) to `Float`
// in the SAME change, together with the Python Strawberry counterparts,
// AND give the web client's mergeToSpark (or equivalent) a real
// null-handling branch first -- see CHAOS-4658.
func ExecuteTimeseries(ctx context.Context, client QueryClient, q compiledQuery, dimensionName, measureName string) ([]model.TimeseriesResult, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	order := []string{}
	buckets := map[string][]model.TimeseriesBucket{}
	for rows.Next() {
		// Scan the DRIVER's type, convert after. graphqldate.Date has
		// no Scan method, so it is not an sql.Scanner and the native
		// Date.ScanRow rejects it outright -- every non-empty result
		// failed. Precedent: reviewedges.go:152.
		var bucketDay time.Time
		var dimValue string
		var value *float64 // nullable scan destination -- see this func's doc comment (CHAOS-4657)
		if scanErr := rows.Scan(&bucketDay, &dimValue, &value); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		bucket := graphqldate.New(bucketDay)
		if _, seen := buckets[dimValue]; !seen {
			order = append(order, dimValue)
		}
		buckets[dimValue] = append(buckets[dimValue], model.TimeseriesBucket{Date: bucket, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	out := make([]model.TimeseriesResult, 0, len(order))
	for _, dimValue := range order {
		out = append(out, model.TimeseriesResult{
			Dimension:      dimensionName,
			DimensionValue: dimValue,
			Measure:        measureName,
			Buckets:        buckets[dimValue],
		})
	}
	return out, nil
}
