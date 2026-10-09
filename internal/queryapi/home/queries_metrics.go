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

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
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
	if table == changefailure.Table {
		// change failure rate is computed from the day's counts, never read
		// as a stored ratio.
		valueColumns = changefailure.CountColumns
	}
	var valueProjections []string
	if isPRRework(table, column) {
		// The rework ratio is computed from the day's counts, never read as a
		// stored ratio. The counts are Nullable (a row written before they
		// existed holds NULL), and argMax skips a NULL argument: it would take
		// a count of an OLDER row of the day. The tuple keeps the NULL of the
		// newest row, so all four counts come from one generation.
		valueColumns = nil
		for _, vc := range prrework.CountColumns {
			valueProjections = append(valueProjections, fmt.Sprintf("tupleElement(argMax(tuple(%s), computed_at), 1) AS %s", vc, vc))
		}
	}
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
            GROUP BY %s%s
        )`, strings.Join(naturalKey, ",\n                "), strings.Join(valueProjections, ",\n                "),
		table, startParam, endParam, scopeFilter, strings.Join(naturalKey, ", "), liveRowHaving(table))
}

// liveRowHaving keeps a key of a team-keyed daily table only when its newest
// row is a measurement (see package liverow). A retraction row is the newest
// row of a key the compute no longer produces: it is not a sample of an
// average, not a row of the has-data count and not a driver to name. Without
// this, a bare argMax over a Nullable column also skips the NULL of the
// retraction row and serves the older value it replaced.
func liveRowHaving(table string) string {
	if !liverow.Registered(table) {
		return ""
	}
	return "\n            HAVING " + liverow.NewestPredicate(table, "")
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
	if isPRRework(table, column) {
		// NULL when the window holds no reviewed pull request: a pull request
		// with no review data says nothing about rework.
		return prrework.WindowRateSQL
	}
	if table == changefailure.Table {
		// NULL when the window is not applicable (no deployment) or unknown
		// (no incident evidence): missing is not healthy.
		return changefailure.WindowRateSQL
	}
	return fmt.Sprintf("toFloat64(%s(%s))", aggregator, column)
}

type dayValueRow struct {
	Day   time.Time
	Value float64
}

// metricValue preserves whether the aggregate read found any source row.
// ClickHouse returns one aggregate row for an empty source, where sum is 0
// and avg is NaN; neither value proves that the metric was reported.
type metricValue struct {
	Value   float64
	HasData bool
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
		var day time.Time
		var value *float64
		if err := rows.Scan(&day, &value); err != nil {
			return nil, fmt.Errorf("home: fetch_metric_series scan: %w", err)
		}
		if value == nil {
			// A day with no value of the metric is left out of the series,
			// never drawn as 0: a mean over days whose stored value is NULL (no
			// reviewed pull request, no completed item), a weighted ratio with
			// no weight, a change failure rate with no deployment or no
			// incident evidence.
			continue
		}
		out = append(out, dayValueRow{Day: day, Value: *value})
	}
	return out, rows.Err()
}

// fetchMetricValue ports fetch_metric_value (api/queries/metrics.py:
// 155-198).
func fetchMetricValue(ctx context.Context, client QueryClient, table, column string, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, aggregator, orgID string) (metricValue, error) {
	valueExpr := metricValueExpression(table, column, aggregator)
	var query string
	if _, dedup := dedupByComputedAt[table]; dedup {
		fromClause := metricFromClause(table, column, scopeFilter, "start_day", "end_day")
		query = fmt.Sprintf(`
        SELECT
			toInt64(count()) AS row_count,
            %s AS value
        FROM %s
    `, valueExpr, fromClause)
	} else {
		query = fmt.Sprintf(`
        SELECT
			toInt64(count()) AS row_count,
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
		return metricValue{}, fmt.Errorf("home: fetch_metric_value query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return metricValue{}, rows.Err()
	}
	var rowCount int64
	var value *float64
	if err := rows.Scan(&rowCount, &value); err != nil {
		return metricValue{}, fmt.Errorf("home: fetch_metric_value scan: %w", err)
	}
	if value == nil {
		// An undefined aggregate is no data, whatever the row count: the
		// window's rows hold no value of the metric (see fetchMetricSeries).
		// Scanned into a plain float64 it would read as 0 with data, a
		// measured zero that nobody measured.
		return metricValue{}, rows.Err()
	}
	return metricValue{Value: *value, HasData: rowCount > 0}, rows.Err()
}

// fetchChangeFailureView reads the window's summed change-failure counts for
// the scope and the number of stored rows behind them, from the newest version
// of each repository and day (the same deduplicated source as the series).
func fetchChangeFailureView(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string) (changefailure.View, error) {
	query := fmt.Sprintf(`
        SELECT %s
        FROM %s
    `, changefailure.ViewSumsSQL, metricFromClause(changefailure.Table, "change_failure_rate", scopeFilter, "start_day", "end_day"))
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return changefailure.View{}, fmt.Errorf("home: fetch_change_failure_view query: %w", err)
	}
	defer rows.Close()
	var view changefailure.View
	if rows.Next() {
		if err := rows.Scan(changefailure.ViewScanDest(&view)...); err != nil {
			return changefailure.View{}, fmt.Errorf("home: fetch_change_failure_view scan: %w", err)
		}
	}
	return view, rows.Err()
}

// isPRRework says that a metric spec is the pull request rework ratio: the
// one repo_metrics_daily metric that is a ratio of stored counts.
func isPRRework(table, column string) bool {
	return table == prrework.Table && column == prrework.DeprecatedRatioColumn
}

// fetchPRReworkView reads the window's summed rework counts for the scope and
// the number of stored rows that hold counts, from the newest version of each
// repository and day (the same deduplicated source as the series).
func fetchPRReworkView(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string) (prrework.View, error) {
	query := fmt.Sprintf(`
        SELECT %s
        FROM %s
    `, prrework.ViewSumsSQL, metricFromClause(prrework.Table, prrework.DeprecatedRatioColumn, scopeFilter, "start_day", "end_day"))
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return prrework.View{}, fmt.Errorf("home: fetch_pr_rework_view query: %w", err)
	}
	defer rows.Close()
	var view prrework.View
	if rows.Next() {
		if err := rows.Scan(prrework.ViewScanDest(&view)...); err != nil {
			return prrework.View{}, fmt.Errorf("home: fetch_pr_rework_view scan: %w", err)
		}
	}
	return view, rows.Err()
}

// fetchBlockedHours ports fetch_blocked_hours (api/queries/metrics.py:
// 207-248).
func fetchBlockedHours(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string) (float64, []dayValueRow, bool, error) {
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
            GROUP BY day, provider, work_scope_id, team_id, status%s
        )
        GROUP BY day
        ORDER BY day
    `, scopeFilter, liveRowHaving("work_item_state_durations_daily"))
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return 0, nil, false, fmt.Errorf("home: fetch_blocked_hours query: %w", err)
	}
	defer rows.Close()

	var out []dayValueRow
	total := 0.0
	for rows.Next() {
		var row dayValueRow
		if err := rows.Scan(&row.Day, &row.Value); err != nil {
			return 0, nil, false, fmt.Errorf("home: fetch_blocked_hours scan: %w", err)
		}
		total += row.Value
		out = append(out, row)
	}
	return total, out, len(out) > 0, rows.Err()
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
	if isPRRework(table, column) {
		return fetchRateDriverDelta(ctx, client, prrework.WindowRateSQL, groupBy, currentFrom, previousFrom, startDay, endDay, compareStart, compareEnd, scopeBindings, orgID, limit)
	}
	if table == changefailure.Table {
		return fetchRateDriverDelta(ctx, client, changefailure.WindowRateSQL, groupBy, currentFrom, previousFrom, startDay, endDay, compareStart, compareEnd, scopeBindings, orgID, limit)
	}
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

// fetchRateDriverDelta is fetchMetricDriverDelta for a rate that is a ratio
// of summed counts (change failure rate, the pull request rework ratio):
// each group's value is the window rate of its own summed counts (rateSQL),
// and a group whose current rate is undefined (no deployment or no incident
// evidence; no reviewed pull request) is not a driver. A group with no
// defined previous rate gets a 0 delta, like a previous value of 0.
func fetchRateDriverDelta(ctx context.Context, client QueryClient, rateSQL, groupBy, currentFrom, previousFrom string, startDay, endDay, compareStart, compareEnd time.Time, scopeBindings []dhclickhouse.Binding, orgID string, limit int) ([]driverRow, error) {
	query := fmt.Sprintf(`
        SELECT
            current.id AS id,
            assumeNotNull(current.value) AS value,
            CASE WHEN ifNull(previous.value, 0) = 0 THEN 0 ELSE (assumeNotNull(current.value) - assumeNotNull(previous.value)) / assumeNotNull(previous.value) * 100 END AS delta_pct
        FROM (
            SELECT %s AS id, %s AS value
            FROM %s
            GROUP BY %s
        ) AS current
        LEFT JOIN (
            SELECT %s AS id, %s AS value
            FROM %s
            GROUP BY %s
        ) AS previous ON current.id = previous.id
        WHERE current.value IS NOT NULL
        ORDER BY delta_pct DESC
        LIMIT {limit:UInt32}
    `, groupBy, rateSQL, currentFrom, groupBy, groupBy, rateSQL, previousFrom, groupBy)
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
		return nil, fmt.Errorf("home: fetch_change_failure_driver_delta query: %w", err)
	}
	defer rows.Close()

	var out []driverRow
	for rows.Next() {
		var id string
		var value, deltaPct float64
		if err := rows.Scan(&id, &value, &deltaPct); err != nil {
			return nil, fmt.Errorf("home: fetch_change_failure_driver_delta scan: %w", err)
		}
		if id != "" {
			out = append(out, driverRow{ID: id})
		}
	}
	return out, rows.Err()
}
