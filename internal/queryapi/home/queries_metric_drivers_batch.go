package home

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
)

// driverBatchMetric is one metric of the batched driver read with the scope
// filter of its scope level (scopeFilterForMetric).
type driverBatchMetric struct {
	Metric      string
	ScopeFilter string
	// FromZero is a value that rose from a measured 0: its drivers rank by the absolute rise (driverRank).
	FromZero bool
	// ShareSQL is the team's share of each repository that weighs the value of a repository sum (scopeShareForMetric).
	ShareSQL string
}

// fetchMetricDriversBatch is fetchMetricDriverDelta for several metrics in ONE
// statement: the same per-metric select (same window rule, same delta rule),
// each tagged with its metric key, unioned, ranked per metric and cut at
// limit rows per metric (LIMIT n BY metric). The row count is bounded in the
// SQL by limit times the number of metrics, so the 1,000-row cap of the query
// client is never reached. A metric without a driver row is absent from the
// result. Ties on the rank break by id, so one request names the same
// drivers twice.
//
// fetchMetricDriverDelta is left as it is: its caller (the summary sentence)
// reads one metric and orders ties by nothing.
func fetchMetricDriversBatch(ctx context.Context, client QueryClient, batch []driverBatchMetric, startDay, endDay, compareStart, compareEnd time.Time, scopeBindings []dhclickhouse.Binding, orgID string, limit int) (map[string][]driverRow, error) {
	if len(batch) == 0 {
		return map[string][]driverRow{}, nil
	}
	parts := make([]string, 0, len(batch))
	for _, m := range batch {
		parts = append(parts, metricDriverSelect(m))
	}
	query := fmt.Sprintf(`
        SELECT metric, id
        FROM (
%s
        )
        ORDER BY metric, rank_value DESC, id
        LIMIT {limit_n:UInt32} BY metric
    `, strings.Join(parts, "\n            UNION ALL\n"))
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
		return nil, fmt.Errorf("home: fetch_metric_drivers_batch query: %w", err)
	}
	defer rows.Close()

	out := map[string][]driverRow{}
	for rows.Next() {
		var metric, id string
		if err := rows.Scan(&metric, &id); err != nil {
			return nil, fmt.Errorf("home: fetch_metric_drivers_batch scan: %w", err)
		}
		if id != "" {
			out[metric] = append(out[metric], driverRow{ID: id})
		}
	}
	return out, rows.Err()
}

// metricDriverSelect is the select of fetchMetricDriverDelta / fetchRateDriverDelta
// for one metric, without the ORDER BY and LIMIT, with the metric key as a
// column and the rank as rank_value. Only the metric's own spec (table, column,
// group, reducer, status) and the move of its value shape it.
func metricDriverSelect(m driverBatchMetric) string {
	table, column, groupBy := metricTable(m.Metric), metricColumn(m.Metric), metricGroup(m.Metric)
	scopeFilter := m.ScopeFilter + driverStatusFilter(table, column)
	currentFrom := metricFromClause(table, column, scopeFilter, "start_day", "end_day", false)
	previousFrom := metricFromClause(table, column, scopeFilter, "compare_start", "compare_end", false)
	value, rate := driverValueSQL(table, column, m.ShareSQL)
	rank, alias, where := driverRank(m.FromZero, driverSums(table, column), rate)
	return fmt.Sprintf(`            SELECT metric, id, %s AS rank_value FROM (
                SELECT
                    '%s' AS metric,
                    toString(current.id) AS id,
                    toFloat64(%s) AS %s
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
                WHERE %s
            )`, alias, m.Metric, rank, alias, groupBy, value, currentFrom, groupBy, groupBy, value, previousFrom, groupBy, where)
}

// driverValueSQL is the value of one driver group in a window: the value's own expression (metricValueExpression),
// with the metric's reducer and, for a repository sum under a team scope, the same share weight (shareSQL). rate is
// true for a rate metric, whose value can be NULL.
func driverValueSQL(table, column, shareSQL string) (value string, rate bool) {
	rate = isPRRework(table, column) || table == changefailure.Table || isPipelineSuccess(table, column)
	return metricValueExpression(table, column, driverAggregator(table, column), shareSQL), rate
}

// driverAggregator is the reducer of the Home value of this table and column (metricSpec.Aggregator); avg for a table
// and column no metric reads.
func driverAggregator(table, column string) string {
	for _, m := range metrics {
		if m.Table == table && m.Column == column {
			return m.Aggregator
		}
	}
	return "avg"
}

// driverSums says whether the Home value of this table and column is a sum: its drivers sum too, as the value does.
func driverSums(table, column string) bool {
	return driverAggregator(table, column) == "sum"
}

// driverShareSQL is the share weight of a metric's value for the request (scopeShareForMetric), for its driver read.
func driverShareSQL(metric string, f Filters, orgID string, asOf time.Time) (string, []dhclickhouse.Binding) {
	for _, spec := range metrics {
		if spec.Metric == metric {
			return scopeShareForMetric(spec, f, orgID, "repo_id", asOf)
		}
	}
	return "", nil
}

// driverStatusFilter is the status predicate of the value: Blocked Work sums the hours in the blocked status only
// (fetchBlockedHours), so its drivers read only those rows.
func driverStatusFilter(table, column string) string {
	if table == metricTable("blocked_work") && column == metricColumn("blocked_work") {
		return "\n              AND status = 'blocked'"
	}
	return ""
}

// driverRank is the rank of a driver row, its column name, and the rows that are drivers.
//
// A percent move ranks by the percent (deltarule.DriverPercentSQL): a group with no row or no value in a window has
// none. A rise from a measured 0 has no percent: it ranks by the absolute rise. A summed value counts a group with no
// row in the prior window as 0, as the value's own sum does, so that group is a driver of the rise. Any other value
// (an average, a rate) needs a measured 0 in the prior window: a stored row whose value is 0, not NULL.
func driverRank(fromZero, sums, rate bool) (rank, alias, where string) {
	if !fromZero {
		where = "delta_pct IS NOT NULL"
		if rate {
			where = "current.value IS NOT NULL AND delta_pct IS NOT NULL"
		}
		return deltarule.DriverPercentSQL("current.value", "previous.value", "previous.present"), "delta_pct", where
	}
	where = "current.value > 0 AND previous.present = 1 AND previous.value IS NOT NULL AND previous.value = 0"
	if sums {
		where = "current.value > 0 AND (previous.present = 0 OR previous.value = 0)"
	}
	return "current.value - if(previous.present = 1, previous.value, 0)", "delta_abs", where
}
