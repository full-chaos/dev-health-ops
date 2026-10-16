package home

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
)

// driverBatchMetric is one metric of the batched driver read with the scope
// filter of its scope level (scopeFilterForMetric).
type driverBatchMetric struct {
	Metric      string
	ScopeFilter string
}

// fetchMetricDriversBatch is fetchMetricDriverDelta for several metrics in ONE
// statement: the same per-metric select (same window rule, same delta rule),
// each tagged with its metric key, unioned, ranked per metric and cut at
// limit rows per metric (LIMIT n BY metric). The row count is bounded in the
// SQL by limit times the number of metrics, so the 1,000-row cap of the query
// client is never reached. A metric without a driver row is absent from the
// result. Ties on the percent break by id, so one request names the same
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
        ORDER BY metric, delta_pct DESC, id
        LIMIT {limit:UInt32} BY metric
    `, strings.Join(parts, "\n            UNION ALL\n"))
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
// column. Only the metric's own spec (table, column, group) shapes it.
func metricDriverSelect(m driverBatchMetric) string {
	table, column, groupBy := metricTable(m.Metric), metricColumn(m.Metric), metricGroup(m.Metric)
	currentFrom := metricFromClause(table, column, m.ScopeFilter, "start_day", "end_day")
	previousFrom := metricFromClause(table, column, m.ScopeFilter, "compare_start", "compare_end")
	pct := deltarule.DriverPercentSQL("current.value", "previous.value", "previous.present")
	value, where := "avg("+column+")", "delta_pct IS NOT NULL"
	switch {
	case isPRRework(table, column):
		value, where = prrework.WindowRateSQL, "current.value IS NOT NULL AND delta_pct IS NOT NULL"
	case table == changefailure.Table:
		value, where = changefailure.WindowRateSQL, "current.value IS NOT NULL AND delta_pct IS NOT NULL"
	}
	return fmt.Sprintf(`            SELECT metric, id, delta_pct FROM (
                SELECT
                    '%s' AS metric,
                    toString(current.id) AS id,
                    toFloat64(%s) AS delta_pct
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
            )`, m.Metric, pct, groupBy, value, currentFrom, groupBy, groupBy, value, previousFrom, groupBy, where)
}
