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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"slices"
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
//
// oneTeam is a read for exactly one team (oneTeamScope): a table with team rows
// is then read through the served team view (CHAOS-14379), one row per key, and
// the argMax/GROUP BY below runs over that row.
func metricFromClause(table, column, scopeFilter, startParam, endParam string, oneTeam bool) string {
	naturalKey, dedup := dedupByComputedAt[table]
	if !dedup {
		return table
	}
	source, teamView := "", false
	if oneTeam {
		source, teamView = teamViewSource(table, fmt.Sprintf("day >= {%s:Date} AND day < {%s:Date}%s", startParam, endParam, scopeFilter))
	}
	if !teamView {
		source = table
	}
	if !teamView && rowview.Has(table) {
		// Each item once in the organization (CHAOS-8863): legacy rows and the org view. row_view is part of the
		// natural key above, so a legacy retraction row never merges with an org row under argMax.
		scopeFilter += "\n              AND " + rowview.OrgPredicate(table, "")
	}
	valueColumns := []string{column}
	if isPipelineSuccess(table, column) {
		// The rate is computed from the day's completed-run counts, never read as a stored ratio.
		valueColumns = []string{"success_count", "failure_count"}
	}
	if table == changefailure.Table {
		// change failure rate is computed from the day's counts, never read
		// as a stored ratio.
		valueColumns = changefailure.CountColumns
	}
	if isPRRework(table, column) {
		// The rework ratio is computed from the day's counts, never read as a stored ratio.
		valueColumns = prrework.CountColumns
	}
	return newestVersionSource(table, valueColumns, source, fmt.Sprintf(`WHERE day >= {%s:Date} AND day < {%s:Date}
            %s
              AND org_id = {org_id:String}`, startParam, endParam, scopeFilter), strings.Join(naturalKey, ", "))
}

// newestVersionSource is one row per key of table: ONE version picked by rowview.VersionOrder (the newest computed_at,
// then the hash of the measures, so a computed_at tie serves one version whatever the insert order), with the value
// columns and the live-row columns of that version. A Nullable column keeps the picked version's NULL. The live-row rule
// is read from the picked row: a key whose picked version is a retraction row leaves the result.
func newestVersionSource(table string, valueColumns []string, source, where, key string) string {
	columns := append([]string(nil), valueColumns...)
	live := ""
	if liverow.Registered(table) {
		for _, column := range liverow.Columns(table) {
			if !slices.Contains(columns, column) {
				columns = append(columns, column)
			}
		}
		live = "\n        WHERE " + liverow.Predicate(table, "newest")
	}
	return fmt.Sprintf(`(
        SELECT * FROM (
            SELECT
                %s,
                %s
            FROM %s
            %s
            GROUP BY %s
        ) AS newest%s
    )`, strings.ReplaceAll(key, ", ", ",\n                "), rowview.VersionPick(table, columns), source, where, key, live)
}

// teamViewSource is the served team view of a table with team rows (CHAOS-14379), aliased as the table: one row per
// key, the newest row of each view of the key, a live row first, then team, org, legacy (rowview.ServedTeamSource). ok
// is false for a table without team rows. Each call names its table as a literal, so a census reads the table.
func teamViewSource(table, where string) (source string, ok bool) {
	switch table {
	case "work_item_metrics_daily":
		return rowview.ServedTeamSource("work_item_metrics_daily", where, rowview.WorkItemMetricsNewestOrder) + " AS work_item_metrics_daily", true
	case "work_item_state_durations_daily":
		return rowview.ServedTeamSource("work_item_state_durations_daily", where, rowview.StateDurationsNewestOrder) + " AS work_item_state_durations_daily", true
	}
	return "", false
}

// metricValueExpression ports _metric_value_expression (api/queries/
// metrics.py:201-204).
func metricValueExpression(table, column, aggregator, shareSQL string) string {
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
	if isPipelineSuccess(table, column) {
		return pipelineSuccessWindowRateSQL
	}
	if aggregator == "sum" && shareSQL != "" {
		// A team scope sums each repository at the share of it the team holds (teamscope.RepoShare, CHAOS-9413).
		return fmt.Sprintf("toFloat64(sum(%s * %s))", column, shareSQL)
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
func fetchMetricSeries(ctx context.Context, client QueryClient, table, column string, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, aggregator, orgID, shareSQL string, oneTeam bool) ([]dayValueRow, error) {
	valueExpr := metricValueExpression(table, column, aggregator, shareSQL)
	var query string
	if _, dedup := dedupByComputedAt[table]; dedup {
		fromClause := metricFromClause(table, column, scopeFilter, "start_day", "end_day", oneTeam)
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
func fetchMetricValue(ctx context.Context, client QueryClient, table, column string, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, aggregator, orgID, shareSQL string, oneTeam bool) (metricValue, error) {
	valueExpr := metricValueExpression(table, column, aggregator, shareSQL)
	var query string
	if _, dedup := dedupByComputedAt[table]; dedup {
		fromClause := metricFromClause(table, column, scopeFilter, "start_day", "end_day", oneTeam)
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
    `, changefailure.ViewSumsSQL, metricFromClause(changefailure.Table, "change_failure_rate", scopeFilter, "start_day", "end_day", false))
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

// pipelineSuccessWindowRateSQL is CI Success Rate over the rows of testops_pipeline_metrics_daily already
// deduplicated to the newest computed_at per (org_id, repo_id, day): the window's successes over its COMPLETED runs
// (success plus failure; cancelled, skipped and running runs are out), never a mean of the stored rates. The same
// table and rule as the TestOps Pipelines series and the Delivery Risk factor. NULL when no run completed: missing,
// not 0 and not 100.
const pipelineSuccessWindowRateSQL = `if(sum(success_count) + sum(failure_count) = 0, NULL, toFloat64(sum(success_count)) / toFloat64(sum(success_count) + sum(failure_count)))`

func isPipelineSuccess(table, column string) bool {
	return table == "testops_pipeline_metrics_daily" && column == "success_count"
}

// fetchPRReworkView reads the window's summed rework counts for the scope and
// the number of stored rows that hold counts, from the newest version of each
// repository and day (the same deduplicated source as the series).
func fetchPRReworkView(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string) (prrework.View, error) {
	query := fmt.Sprintf(`
        SELECT %s
        FROM %s
    `, prrework.ViewSumsSQL, metricFromClause(prrework.Table, prrework.DeprecatedRatioColumn, scopeFilter, "start_day", "end_day", false))
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
//
// oneTeam reads the served team view of the one team (CHAOS-14379), as
// metricFromClause does.
func fetchBlockedHours(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilter string, scopeBindings []dhclickhouse.Binding, orgID string, oneTeam bool) (float64, []dayValueRow, bool, error) {
	source, view := "work_item_state_durations_daily", "\n              AND "+rowview.OrgPredicate("work_item_state_durations_daily", "")
	if oneTeam {
		source, view = rowview.ServedTeamSource("work_item_state_durations_daily",
			"day >= {start_day:Date} AND day < {end_day:Date} AND status = 'blocked'"+scopeFilter, rowview.StateDurationsNewestOrder)+" AS work_item_state_durations_daily", ""
	}
	query := fmt.Sprintf(`
        SELECT
            day,
            sum(duration_hours) AS value
        FROM %s
        GROUP BY day
        ORDER BY day
    `, newestVersionSource("work_item_state_durations_daily", []string{"duration_hours"}, source, fmt.Sprintf(`WHERE day >= {start_day:Date} AND day < {end_day:Date}
              AND status = 'blocked'
            %s
              AND org_id = {org_id:String}%s`, scopeFilter, view), "day, provider, work_scope_id, team_id, status, row_view"))
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
	currentFrom := driverDeltaSource(table, column, scopeFilter, "start_day", "end_day")
	previousFrom := driverDeltaSource(table, column, scopeFilter, "compare_start", "compare_end")
	if isPRRework(table, column) {
		return fetchRateDriverDelta(ctx, client, prrework.WindowRateSQL, groupBy, currentFrom, previousFrom, startDay, endDay, compareStart, compareEnd, scopeBindings, orgID, limit)
	}
	if table == changefailure.Table {
		return fetchRateDriverDelta(ctx, client, changefailure.WindowRateSQL, groupBy, currentFrom, previousFrom, startDay, endDay, compareStart, compareEnd, scopeBindings, orgID, limit)
	}
	if isPipelineSuccess(table, column) {
		return fetchRateDriverDelta(ctx, client, pipelineSuccessWindowRateSQL, groupBy, currentFrom, previousFrom, startDay, endDay, compareStart, compareEnd, scopeBindings, orgID, limit)
	}
	query := fmt.Sprintf(`
        SELECT
            current.id AS id,
            current.value AS value,
            %s AS delta_pct
        FROM (
            SELECT %s AS id, avg(%s) AS value
            FROM %s
            GROUP BY %s
        ) AS current
        LEFT JOIN (
            SELECT %s AS id, avg(%s) AS value, toUInt8(1) AS present
            FROM %s
            GROUP BY %s
        ) AS previous ON current.id = previous.id
        WHERE delta_pct IS NOT NULL
        ORDER BY delta_pct DESC
        LIMIT {limit_n:UInt32}
    `, deltarule.DriverPercentSQL("current.value", "previous.value", "previous.present"), groupBy, column, currentFrom, groupBy, groupBy, column, previousFrom, groupBy)
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "compare_start", Value: formatDay(compareStart)},
		{Name: "compare_end", Value: formatDay(compareEnd)},
		{Name: "org_id", Value: orgID},
		{Name: "limit_n", Value: uint32(limit)},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_metric_driver_delta query: %w", err)
	}
	defer rows.Close()

	var out []driverRow
	for rows.Next() {
		var id string
		var value, deltaPct *float64
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
// evidence; no reviewed pull request) is not a driver, and neither is a group
// without a delta: a group with no row in the comparison window, or with no
// defined previous rate, is not named as the driver of a percent it has no
// part in (deltarule.DriverPercentSQL).
func fetchRateDriverDelta(ctx context.Context, client QueryClient, rateSQL, groupBy, currentFrom, previousFrom string, startDay, endDay, compareStart, compareEnd time.Time, scopeBindings []dhclickhouse.Binding, orgID string, limit int) ([]driverRow, error) {
	query := fmt.Sprintf(`
        SELECT
            current.id AS id,
            assumeNotNull(current.value) AS value,
            %s AS delta_pct
        FROM (
            SELECT %s AS id, %s AS value
            FROM %s
            GROUP BY %s
        ) AS current
        LEFT JOIN (
            SELECT %s AS id, %s AS value, toUInt8(1) AS present
            FROM %s
            GROUP BY %s
        ) AS previous ON current.id = previous.id
        WHERE current.value IS NOT NULL AND delta_pct IS NOT NULL
        ORDER BY delta_pct DESC
        LIMIT {limit_n:UInt32}
    `, deltarule.DriverPercentSQL("current.value", "previous.value", "previous.present"), groupBy, rateSQL, currentFrom, groupBy, groupBy, rateSQL, previousFrom, groupBy)
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "compare_start", Value: formatDay(compareStart)},
		{Name: "compare_end", Value: formatDay(compareEnd)},
		{Name: "org_id", Value: orgID},
		{Name: "limit_n", Value: uint32(limit)},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_change_failure_driver_delta query: %w", err)
	}
	defer rows.Close()

	var out []driverRow
	for rows.Next() {
		var id string
		var value, deltaPct *float64
		if err := rows.Scan(&id, &value, &deltaPct); err != nil {
			return nil, fmt.Errorf("home: fetch_change_failure_driver_delta scan: %w", err)
		}
		if id != "" {
			out = append(out, driverRow{ID: id})
		}
	}
	return out, rows.Err()
}
