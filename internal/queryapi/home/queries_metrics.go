// ClickHouse readers for the per-metric value/series/driver-delta and
// blocked-hours reads -- ports api/queries/metrics.py and the metric
// slice of api/queries/explain.py verbatim, natural-key dedup included
// (both already correct against the daily-family ReplacingMergeTree
// tables' prod sorting keys -- prod system.tables confirmation covers these
// same tables -- so this is a straight translation, no divergence).
package home

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func formatDay(t time.Time) string { return t.Format("2006-01-02") }

// metricFromClause ports _metric_from_clause (api/queries/metrics.py:
// 61-104). startParam/endParam name the bound day-range params so the
// same shape serves both the primary window (start_day/end_day) and the
// driver-delta comparison window (compare_start/compare_end).
func metricFromClause(table, column, scopeFilter, startParam, endParam string) string {
	naturalKey, dedup := dedupByComputedAt[table]
	if !dedup {
		return table
	}
	valueColumns := []string{column}
	if table == "repo_metrics_daily" && column == "pr_rework_ratio" {
		// The weighted aggregate also consumes prs_merged; both must come
		// from the same latest daily generation.
		valueColumns = append(valueColumns, "prs_merged")
	}
	var valueProjections []string
	for _, vc := range valueColumns {
		valueProjections = append(valueProjections, fmt.Sprintf("argMax(%s, computed_at) AS %s", vc, vc))
	}
	return fmt.Sprintf(`(
            SELECT
                %s,
                %s
            FROM %s
            WHERE day >= {%s:Date} AND day < {%s:Date}
            %s
              AND org_id = {org_id:String}
            GROUP BY %s
        )`, strings.Join(naturalKey, ",\n                "), strings.Join(valueProjections, ",\n                "),
		table, startParam, endParam, scopeFilter, strings.Join(naturalKey, ", "))
}

// metricValueExpression ports _metric_value_expression (api/queries/
// metrics.py:201-204).
func metricValueExpression(table, column, aggregator string) string {
	// Every expression is wrapped in toFloat64(...): sum()/count() over an
	// integer-typed ClickHouse column (UInt32/UInt64 -- every column this
	// package's metric specs sum is one) returns UInt64, which the driver
	// refuses to scan into the *float64 destination every caller here
	// uses (confirmed live: "converting UInt64 to *float64 is
	// unsupported"), matching quadrant.go's own identical convention.
	// avg() already returns Float64 regardless of the summed column's own
	// width, so the cast is a no-op there.
	if table == "repo_metrics_daily" && column == "pr_rework_ratio" {
		return "toFloat64(SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0))"
	}
	return fmt.Sprintf("toFloat64(%s(%s))", aggregator, column)
}

type dayValueRow struct {
	Day   time.Time
	Value float64
}

// fetchMetricSeries ports fetch_metric_series (api/queries/metrics.py:
// 107-152).
func fetchMetricSeries(ctx context.Context, client QueryClient, table, column string, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, aggregator, orgID string) ([]dayValueRow, error) {
	valueExpr := metricValueExpression(table, column, aggregator)
	var query string
	if _, dedup := dedupByComputedAt[table]; dedup {
		fromClause := metricFromClause(table, column, scopeFilter, "start_day", "end_day")
		query = fmt.Sprintf(`
        SELECT
            day,
            %s AS value
        FROM %s
        GROUP BY day
        ORDER BY day
    `, valueExpr, fromClause)
	} else {
		query = fmt.Sprintf(`
        SELECT
            day,
            %s AS value
        FROM %s
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
        %s
          AND org_id = {org_id:String}
        GROUP BY day
        ORDER BY day
    `, valueExpr, table, scopeFilter)
	}
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_metric_series query: %w", err)
	}
	defer rows.Close()

	var out []dayValueRow
	for rows.Next() {
		var row dayValueRow
		if err := rows.Scan(&row.Day, &row.Value); err != nil {
			return nil, fmt.Errorf("home: fetch_metric_series scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// fetchMetricValue ports fetch_metric_value (api/queries/metrics.py:
// 155-198).
func fetchMetricValue(ctx context.Context, client QueryClient, table, column string, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, aggregator, orgID string) (float64, error) {
	valueExpr := metricValueExpression(table, column, aggregator)
	var query string
	if _, dedup := dedupByComputedAt[table]; dedup {
		fromClause := metricFromClause(table, column, scopeFilter, "start_day", "end_day")
		query = fmt.Sprintf(`
        SELECT
            %s AS value
        FROM %s
    `, valueExpr, fromClause)
	} else {
		query = fmt.Sprintf(`
        SELECT
            %s AS value
        FROM %s
        WHERE day >= {start_day:Date} AND day < {end_day:Date}
        %s
          AND org_id = {org_id:String}
    `, valueExpr, table, scopeFilter)
	}
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("home: fetch_metric_value query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, rows.Err()
	}
	var value float64
	if err := rows.Scan(&value); err != nil {
		return 0, fmt.Errorf("home: fetch_metric_value scan: %w", err)
	}
	return value, rows.Err()
}

// fetchBlockedHours ports fetch_blocked_hours (api/queries/metrics.py:
// 207-248).
func fetchBlockedHours(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string) (float64, []dayValueRow, error) {
	query := fmt.Sprintf(`
        SELECT
            day,
            sum(duration_hours) AS value
        FROM (
            SELECT
                day,
                provider,
                work_scope_id,
                team_id,
                status,
                argMax(duration_hours, computed_at) AS duration_hours
            FROM work_item_state_durations_daily
            WHERE day >= {start_day:Date} AND day < {end_day:Date}
              AND status = 'blocked'
            %s
              AND org_id = {org_id:String}
            GROUP BY day, provider, work_scope_id, team_id, status
        )
        GROUP BY day
        ORDER BY day
    `, scopeFilter)
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, nil, fmt.Errorf("home: fetch_blocked_hours query: %w", err)
	}
	defer rows.Close()

	var out []dayValueRow
	total := 0.0
	for rows.Next() {
		var row dayValueRow
		if err := rows.Scan(&row.Day, &row.Value); err != nil {
			return 0, nil, fmt.Errorf("home: fetch_blocked_hours scan: %w", err)
		}
		total += row.Value
		out = append(out, row)
	}
	return total, out, rows.Err()
}

type driverRow struct {
	ID string
}

// fetchMetricDriverDelta ports fetch_metric_driver_delta (api/queries/
// explain.py:60-149), the branch this package's one caller (the summary
// sentence's driver-labels lookup) needs: the dedup-aware form, since
// every metric _METRICS declares is in dedupByComputedAt.
//
// A bare SELECT, never a leading WITH: the pinned dev-health-go read-only
// client rejects any statement whose first token is not SELECT
// (clickhouse/client.go's validateReadOnlyStatement), so a WITH-leading
// query never reaches ClickHouse at all -- it returns ErrUnsafeStatement
// before the request does anything useful. The former `current`/
// `previous` CTEs are inlined as FROM subqueries instead, matching
// internal/analytics/flowmatrix.go's own identical fix for the same
// guard (see that file's flowMatrixTeamActivitySelect doc comment).
func fetchMetricDriverDelta(ctx context.Context, client QueryClient, table, column, groupBy string, startDay, endDay, compareStart, compareEnd time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string, limit int) ([]driverRow, error) {
	currentFrom := metricFromClause(table, column, scopeFilter, "start_day", "end_day")
	previousFrom := metricFromClause(table, column, scopeFilter, "compare_start", "compare_end")
	query := fmt.Sprintf(`
        SELECT
            current.id AS id,
            current.value AS value,
            CASE WHEN previous.value = 0 THEN 0 ELSE (current.value - previous.value) / previous.value * 100 END AS delta_pct
        FROM (
            SELECT %s AS id, avg(%s) AS value
            FROM %s
            GROUP BY %s
        ) AS current
        LEFT JOIN (
            SELECT %s AS id, avg(%s) AS value
            FROM %s
            GROUP BY %s
        ) AS previous ON current.id = previous.id
        ORDER BY delta_pct DESC
        LIMIT {limit:UInt32}
    `, groupBy, column, currentFrom, groupBy, groupBy, column, previousFrom, groupBy)
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "compare_start", Value: formatDay(compareStart)},
		{Name: "compare_end", Value: formatDay(compareEnd)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: uint32(limit)},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_metric_driver_delta query: %w", err)
	}
	defer rows.Close()

	var out []driverRow
	for rows.Next() {
		var id string
		var value, deltaPct float64
		if err := rows.Scan(&id, &value, &deltaPct); err != nil {
			return nil, fmt.Errorf("home: fetch_metric_driver_delta scan: %w", err)
		}
		if id != "" {
			out = append(out, driverRow{ID: id})
		}
	}
	return out, rows.Err()
}
