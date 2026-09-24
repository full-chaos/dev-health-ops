package explain

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// dedupNaturalKeys ports api/queries/metrics.py's _DEDUP_BY_COMPUTED_AT,
// restricted to the four tables metricConfigs actually reaches (every
// metricConfigs entry's Table is a key of this map -- confirmed against
// migrations 055/096, see this package's own PR RISK-NOTES for the
// per-table citation). The OTHER five tables that dict registers
// (work_item_user_metrics_daily, cicd_metrics_daily,
// incident_metrics_daily, testops_release_confidence,
// testops_pipeline_stability) are unreachable from /explain and are not
// ported here -- a future metric-config entry that adds one of them must
// extend this map too.
var dedupNaturalKeys = map[string][]string{
	"repo_metrics_daily":              {"day", "repo_id"},
	"work_item_state_durations_daily": {"day", "provider", "work_scope_id", "team_id", "status"},
	"work_item_metrics_daily":         {"day", "provider", "work_scope_id", "team_id"},
	"deploy_metrics_daily":            {"day", "repo_id"},
}

// nullableMetricColumns names every metricConfigs column that is
// Nullable(Float64) in its own table's CREATE TABLE (001_metrics_v2.sql):
// work_item_metrics_daily.cycle_time_p50_hours and
// repo_metrics_daily.pr_first_review_p50_hours. The other six columns
// metricConfigs reaches are all non-nullable (UInt32 or a bare Float64,
// confirmed against the same migration), so a plain argMax is correct
// for them.
var nullableMetricColumns = map[string]bool{
	"cycle_time_p50_hours":      true,
	"pr_first_review_p50_hours": true,
}

// metricValueProjection applies the class ruling (b) dedup fix for a
// Nullable metric column: `argMax(col, version)` on a Nullable column can
// return a STALE non-null version instead of the row with the true
// maximal version, if that latest version's own value happens to be
// NULL -- argMax's candidate selection skips a NULL `arg`, it does not
// track "the value at the max version" the way a non-nullable column
// does. The fix is `(argMax(tuple(col), version)).1`: wrapping the
// Nullable column in a tuple makes argMax compare candidates by
// (value IS NOT NULL, value) tuples instead, so the TRUE latest version
// always wins, NULL included. Python's own reader
// (api/queries/metrics.py's _metric_from_clause) is NOT a correct
// exemplar here (bare argMax on a Nullable column) -- this is a
// declared, required divergence under the class ruling, not a
// data-semantics choice.
func metricValueProjection(column string) string {
	if nullableMetricColumns[column] {
		return fmt.Sprintf("(argMax(tuple(%s), computed_at)).1 AS %s", column, column)
	}
	return fmt.Sprintf("argMax(%s, computed_at) AS %s", column, column)
}

// metricFromClause ports _metric_from_clause (api/queries/metrics.py:
// 61-104), restricted to the dedup branch: every table metricConfigs
// names IS in dedupNaturalKeys (see that map's own doc comment), so
// Python's non-dedup "else" branch (a raw `FROM {table}`) is dead code on
// this route's call path and is not ported -- a future metric-config
// entry naming a table outside dedupNaturalKeys would need it added.
//
// ORG SCOPE + class ruling (a): org_id sits inside this SAME subquery's
// WHERE, before the GROUP BY that collapses to one row per natural key --
// never a filter applied after a cross-tenant scan.
//
// DEDUP + class ruling (b): the value projection is
// metricValueProjection's own argMax/tuple-argMax choice, never a bare
// argMax on a Nullable column (see that function's own doc comment).
//
// startParam/endParam name the bound query params so one function serves
// both the current window (start_day/end_day) and the comparison window
// (compare_start/compare_end), matching Python's own start_param/
// end_param default-override parameters.
func metricFromClause(table, column, scopeFilterSQL, startParam, endParam string) string {
	keys := dedupNaturalKeys[table]
	keyColumns := strings.Join(keys, ",\n        ")
	return fmt.Sprintf(`(
    SELECT
        %s,
        %s
    FROM %s
    WHERE day >= {%s:Date} AND day < {%s:Date}
    %s
      AND org_id = {org_id:String}
    GROUP BY %s
)`, keyColumns, metricValueProjection(column), table, startParam, endParam, scopeFilterSQL, keyColumns)
}

// fetchMetricValue ports fetch_metric_value (api/queries/metrics.py:
// 155-198), restricted to the dedup branch (see metricFromClause's own
// doc comment). Returns float(value or 0.0) already applied -- the
// caller (response.go) applies safeFloat on top, matching Python's own
// two-layer None-then-NaN/Inf handling (fetch_metric_value's inner
// `float(value or 0.0)`, then build_explain_response's own outer
// safe_float call).
func (reader *Reader) fetchMetricValue(ctx context.Context, table, column, aggregator string, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) (float64, error) {
	if reader == nil || reader.client == nil {
		return 0, ErrUnavailable
	}

	fromClause := metricFromClause(table, column, scopeFilterSQL, "start_day", "end_day")
	// toFloat64(...) around the outer aggregator: a
	// sum-aggregator metric over an integer column (deploy_freq's
	// deployments_count, churn's total_loc_touched) makes ClickHouse's
	// sum() return UInt64, not Float64 -- confirmed live, this binary's
	// native driver rejects scanning that into the *float64 destination
	// below outright ("converting UInt64 to **float64 is unsupported"),
	// the same class of mismatch newUnrestrictedReadClickHouseOptions'
	// own doc comment (query_route.go) and quadrant.go's own
	// `toFloat64(%s) AS value` fix already document elsewhere in this
	// binary. avg()-aggregated metrics (review_latency,
	// change_failure_rate) already return Float64/Float32 on their own,
	// so this cast is a no-op for them -- never a value change, only a
	// static, driver-safe destination type.
	query := fmt.Sprintf(`
SELECT
    toFloat64(%s(%s)) AS value
FROM %s
%s
`, aggregator, column, fromClause, settingsMaxExecutionTime())

	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("fetch metric value: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate metric value rows: %w", err)
		}
		return 0.0, nil
	}
	var value *float64
	if err := rows.Scan(&value); err != nil {
		return 0, fmt.Errorf("scan metric value row: %w", err)
	}
	return floatOrZero(value), nil
}

// metricRow is one row fetchMetricContributors/fetchMetricDriverDelta
// returns -- id/value for a contributor row, id/value/delta_pct for a
// driver row (DeltaPct is unused/zero for a contributor row; the caller,
// buildContributor, always overrides a contributor's delta to 0.0
// itself, matching explain.py:240's own `delta_value=0.0` literal).
type metricRow struct {
	ID       string
	Value    float64
	DeltaPct float64
}

// fetchMetricContributors ports fetch_metric_contributors
// (api/queries/explain.py:10-57), dedup branch only (see
// metricFromClause's own doc comment). limit=6 matches explain.py's own
// unqualified call (build_explain_response never overrides the keyword
// default).
//
// aggregator is the metric's OWN config.Aggregator (sum for a count-type
// metric, avg for a ratio/duration-type one) -- the same choice
// fetchMetricValue's headline read already makes. Python's own reader
// hardcodes avg() here regardless of the metric, so a sum-aggregator
// metric's ranking there silently averages a quantity its own headline
// and label present as a total. This is a declared, required divergence,
// not a data-semantics choice.
func (reader *Reader) fetchMetricContributors(ctx context.Context, table, column, groupBy, aggregator string, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]metricRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	const limit = 6

	fromClause := metricFromClause(table, column, scopeFilterSQL, "start_day", "end_day")
	// toFloat64(...) around the outer aggregator: same fix as
	// fetchMetricValue's own doc comment -- a sum-aggregated integer
	// column returns UInt64, which this binary's native driver refuses to
	// scan into a *float64 destination.
	query := fmt.Sprintf(`
SELECT
    toString(%s) AS id,
    toFloat64(%s(%s)) AS value
FROM %s
GROUP BY %s
ORDER BY value DESC
LIMIT {limit:UInt64}
%s
`, groupBy, aggregator, column, fromClause, groupBy, settingsMaxExecutionTime())

	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch metric contributors: %w", err)
	}
	defer rows.Close()

	out := make([]metricRow, 0)
	for rows.Next() {
		var id string
		var value *float64
		if err := rows.Scan(&id, &value); err != nil {
			return nil, fmt.Errorf("scan metric contributor row: %w", err)
		}
		out = append(out, metricRow{ID: id, Value: floatOrZero(value)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metric contributor rows: %w", err)
	}
	return out, nil
}

// fetchMetricDriverDelta ports fetch_metric_driver_delta
// (api/queries/explain.py:60-149), dedup branch only. limit=3 matches
// explain.py's own unqualified call. aggregator is the metric's own
// config.Aggregator -- the same declared divergence from Python's
// hardcoded avg() that fetchMetricContributors' own doc comment states.
// delta_pct is computed IN SQL, the same CASE Python's own query uses (a
// NULL-safe fallback the naive `(current-previous)/previous` would not
// have for previous=0) -- scanned as nullable and denulled the SAME way
// value is, matching where Python's own safe_float(row.get("delta_pct"))
// applies AFTER the SQL layer, not folded into the SQL itself: folding it
// in would change which branch the CASE takes for a NULL current/previous
// pairing (see this package's own PR RISK-NOTES for the worked example
// this avoids).
func (reader *Reader) fetchMetricDriverDelta(ctx context.Context, table, column, groupBy, aggregator string, startDay, endDay, compareStart, compareEnd time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]metricRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	const limit = 3

	currentFrom := metricFromClause(table, column, scopeFilterSQL, "start_day", "end_day")
	previousFrom := metricFromClause(table, column, scopeFilterSQL, "compare_start", "compare_end")
	// current/previous were a `WITH current AS (...), previous AS (...)`
	// CTE pair until this fix: dev-health-go's client-side read-only
	// guard (clickhouse/client.go's validateReadOnlyStatement) requires a
	// statement's FIRST token to be the literal "SELECT", so a query
	// beginning "WITH ..." is rejected before it ever reaches ClickHouse
	// -- ErrUnsafeStatement ("clickhouse runtime: unsafe statement"),
	// wrapped into the generic 503 every /explain request degraded to,
	// with nothing logged (the defect the telemetry sweep in this same change also
	// fixes). Each CTE is referenced exactly once (current in FROM,
	// previous in the LEFT JOIN), so inlining both as ordinary derived-
	// table subqueries is a purely mechanical, semantically identical
	// rewrite -- no materialization or multiple-reference concern applies.
	// toFloat64(...) around each side's outer aggregator: same this defect class
	// fix as fetchMetricValue's own doc comment -- a sum-aggregated
	// integer column returns UInt64, which this binary's native driver
	// refuses to scan into current.value/previous.value's *float64
	// destinations.
	query := fmt.Sprintf(`
SELECT
    current.id AS id,
    current.value AS value,
    CASE WHEN previous.value = 0 THEN 0 ELSE (current.value - previous.value) / previous.value * 100 END AS delta_pct
FROM (
    SELECT toString(%s) AS id, toFloat64(%s(%s)) AS value
    FROM %s
    GROUP BY %s
) AS current
LEFT JOIN (
    SELECT toString(%s) AS id, toFloat64(%s(%s)) AS value
    FROM %s
    GROUP BY %s
) AS previous ON current.id = previous.id
ORDER BY delta_pct DESC
LIMIT {limit:UInt64}
%s
`, groupBy, aggregator, column, currentFrom, groupBy, groupBy, aggregator, column, previousFrom, groupBy, settingsMaxExecutionTime())

	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "compare_start", Value: dateBindingValue(compareStart)},
		{Name: "compare_end", Value: dateBindingValue(compareEnd)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch metric driver delta: %w", err)
	}
	defer rows.Close()

	out := make([]metricRow, 0)
	for rows.Next() {
		var id string
		var value, deltaPctValue *float64
		if err := rows.Scan(&id, &value, &deltaPctValue); err != nil {
			return nil, fmt.Errorf("scan metric driver row: %w", err)
		}
		out = append(out, metricRow{ID: id, Value: floatOrZero(value), DeltaPct: floatOrZero(deltaPctValue)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metric driver rows: %w", err)
	}
	return out, nil
}

// dateBindingValue formats t as "YYYY-MM-DD" for a {name:Date}-typed
// bound parameter -- NOT a raw time.Time, matching the established fix
// in internal/analytics/validate.go's own dateBindingValue (its doc
// comment: six sites shared a {name:Date}-vs-time.Time formatting defect
// before that fix landed). Duplicated here rather than imported, matching
// this binary's own "repeat, don't couple" convention for a helper this
// narrow.
func dateBindingValue(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}
