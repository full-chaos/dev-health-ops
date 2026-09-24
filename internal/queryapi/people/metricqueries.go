// Table-driven metric reads shared by GET /api/v1/people/{person_id}/
// summary and GET /api/v1/people/{person_id}/metric -- ports
// queries/people.py's fetch_person_metric_value (82-112),
// fetch_person_metric_series (115-142) and fetch_person_breakdown
// (145-178), backed by sql/people/person_summary_deltas.sql,
// person_metric_timeseries.sql and person_metric_breakdowns.sql,
// inlined as Go format-string templates the same way search.go's
// searchPeopleQuery already is. See metricconfig.go's own doc comment
// for the {table} dedup fix every one of these three templates carries.
package people

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func formatDay(t time.Time) string {
	return t.Format("2006-01-02")
}

// personMetricValueQuery ports person_summary_deltas.sql, with TWO Go-side
// scan fixes neither Python nor a fixed hand-written query needs, because
// THIS query's aggregate expression is config-driven (metricconfig.go's
// personMetrics table) and spans both `avg()` (over a Nullable(Float64)
// column, e.g. cycle_time_p50_hours) and `sum()` (over a plain UInt32
// column, e.g. items_completed/loc_touched, migrations 001/005) depending
// on which metric is being read:
//  1. `toFloat64(...)`: sum() over a UInt32 column returns UInt64 in
//     ClickHouse; this binary's native driver rejects scanning an
//     unsigned column into a signed/float destination outright (the same
//     class of mismatch internal/queryapi/analytics/
//     investmentquality.go's own row-type doc comment and
//     internal/queryapi/reviewedges/reviewedges.go:145's
//     ReviewsCount both document) -- normalizing the SQL side to Float64,
//     the same fix internal/queryapi/quadrant/quadrant.go's own
//     config-driven `toFloat64(%s) AS value` already applies for the
//     identical "one column, many possible underlying types" shape, lets
//     every call site here scan a plain float64 regardless of which
//     metric ran.
//  2. `coalesce(..., 0)`: UNLIKE person_metric_timeseries.sql/
//     person_metric_breakdowns.sql below (both GROUP BY, so every
//     emitted row already has at least one matching source row), this
//     query has NO GROUP BY: it always returns EXACTLY ONE row, even
//     when zero source rows match the WHERE (a person idle for the whole
//     window, unexceptional and common) -- ClickHouse's own behavior for
//     an aggregate with no GROUP BY over an empty input. avg() over a
//     Nullable column in that shape can come back NULL. Python's own
//     reader has the identical exposure and covers it the same way,
//     defensively, at the call site (`rows[0].get("value") or 0.0`,
//     queries/people.py:112) -- ported here as a `coalesce` in the SQL
//     itself, matching fetch_person_metric_value's own single-row
//     contract, rather than requiring every scan-site here to defend
//     against a NULL scanned into a bare (non-pointer) float64
//     destination.
const personMetricValueQuery = `
SELECT
    coalesce(toFloat64(%s(%s)), 0) AS value
FROM %s
WHERE day >= {start_day:Date} AND day < {end_day:Date}
  AND %s IN {identities:Array(String)}
  %s
  AND org_id = {org_id:String}
%s
`

// fetchPersonMetricValue ports fetch_person_metric_value (queries/
// people.py:82-112): a single aggregate over [startDay, endDay).
func fetchPersonMetricValue(ctx context.Context, client QueryClient, table, column, aggregator, identityColumn string, identities []string, startDay, endDay time.Time, extraWhere, orgID string) (float64, error) {
	query := fmt.Sprintf(personMetricValueQuery, aggregator, column, dedupTable(table), identityColumn, extraWhere, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("people: fetch_person_metric_value query: %w", err)
	}
	defer rows.Close()

	var value float64
	found := false
	if rows.Next() {
		found = true
		if err := rows.Scan(&value); err != nil {
			return 0, fmt.Errorf("people: fetch_person_metric_value scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("people: fetch_person_metric_value: %w", err)
	}
	if !found {
		// `if not rows: return 0.0` (queries/people.py:110-111).
		return 0.0, nil
	}
	return value, nil
}

// personMetricTimeseriesRow is one row fetchPersonMetricSeries returns --
// a (day, value) pair, mirroring the dict fetch_person_metric_series
// (queries/people.py:115-142) returns before _spark_points/
// build_person_metric_response's own transform+safe_float pass.
type personMetricTimeseriesRow struct {
	Day   time.Time
	Value float64
}

// personMetricSeriesQuery ports person_metric_timeseries.sql, with the
// same toFloat64/coalesce fix as personMetricValueQuery's own doc
// comment (the GROUP BY here means an all-NULL/empty group can never be
// emitted, so coalesce is a defensive no-op rather than a load-bearing
// fix, but it costs nothing and keeps every scan site in this file
// scanning the same plain-float64 shape).
const personMetricSeriesQuery = `
SELECT
    day,
    coalesce(toFloat64(%s(%s)), 0) AS value
FROM %s
WHERE day >= {start_day:Date} AND day < {end_day:Date}
  AND %s IN {identities:Array(String)}
  %s
  AND org_id = {org_id:String}
GROUP BY day
ORDER BY day
%s
`

// fetchPersonMetricSeries ports fetch_person_metric_series (queries/
// people.py:115-142): one (day, value) row per day with any data in
// range.
func fetchPersonMetricSeries(ctx context.Context, client QueryClient, table, column, aggregator, identityColumn string, identities []string, startDay, endDay time.Time, extraWhere, orgID string) ([]personMetricTimeseriesRow, error) {
	query := fmt.Sprintf(personMetricSeriesQuery, aggregator, column, dedupTable(table), identityColumn, extraWhere, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_metric_series query: %w", err)
	}
	defer rows.Close()

	out := make([]personMetricTimeseriesRow, 0)
	for rows.Next() {
		var row personMetricTimeseriesRow
		if err := rows.Scan(&row.Day, &row.Value); err != nil {
			return nil, fmt.Errorf("people: fetch_person_metric_series scan: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_person_metric_series: %w", err)
	}
	return out, nil
}

// personBreakdownRow is one row fetchPersonBreakdown returns -- a
// (label, value) pair.
type personBreakdownRow struct {
	Label string
	Value float64
}

// personBreakdownQuery ports person_metric_breakdowns.sql, with the same
// toFloat64/coalesce fix as personMetricValueQuery's own doc comment.
const personBreakdownQuery = `
SELECT
    %s AS label,
    coalesce(toFloat64(%s(%s)), 0) AS value
FROM %s
%s
WHERE day >= {start_day:Date} AND day < {end_day:Date}
  AND %s IN {identities:Array(String)}
  %s
  AND org_id = {org_id:String}
GROUP BY %s
ORDER BY value DESC
LIMIT {limit:UInt64}
%s
`

// fetchPersonBreakdown ports fetch_person_breakdown (queries/people.py:
// 145-178): up to limit (label, value) rows, highest value first.
// breakdownLimit ports the hardcoded default fetch_person_breakdown's own
// signature carries (queries/people.py:158, `limit: int = 12`) -- neither
// build_person_metric_response call site overrides it.
const breakdownLimit = 12

func fetchPersonBreakdown(ctx context.Context, client QueryClient, cfg personBreakdownConfig, identities []string, startDay, endDay time.Time, orgID string) ([]personBreakdownRow, error) {
	joinClause := cfg.JoinClause
	if joinClause != "" {
		joinClause = strings.TrimSpace(joinClause)
	}
	query := fmt.Sprintf(personBreakdownQuery,
		cfg.GroupExpr, cfg.Aggregator, cfg.Column, dedupTable(cfg.Table), joinClause,
		cfg.IdentityColumn, cfg.ExtraWhere, cfg.GroupExpr, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "identities", Value: identities},
		{Name: "limit", Value: breakdownLimit},
		{Name: "org_id", Value: orgID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_breakdown query: %w", err)
	}
	defer rows.Close()

	out := make([]personBreakdownRow, 0)
	for rows.Next() {
		var row personBreakdownRow
		if err := rows.Scan(&row.Label, &row.Value); err != nil {
			return nil, fmt.Errorf("people: fetch_person_breakdown scan: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: fetch_person_breakdown: %w", err)
	}
	return out, nil
}
