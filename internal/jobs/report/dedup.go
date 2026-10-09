package report

import (
	"fmt"
	"strings"
)

// CHAOS-4246: the weekly-report engine charts daily metric rollups that are
// append-only in practice: a re-run of a day adds a new version of a key
// (computed_at later) and merges only eventually, so a bare sum() or avg()
// counts every version. dedupFromSource reads the newest version of each key
// for EVERY table the registry charts. Which key and which version column each
// table has is declared once, with its date column, in tableReads (tablereads.go).
// This Go declaration does not depend on the Python mirror
// (src/dev_health_ops/clickhouse_dedup.py), which is not consulted here.

// sampleCountColumns names, per source table and metric, the column of the
// same row that counts the samples the metric was computed from. A row whose
// count is 0 has no value of the metric: the stored 0 is the default of a
// column that is not nullable, not a measured 0. The daily family
// work_item_issue_type writes such a row of zeros for a key that lost its
// last item, and the compute writes one for a key with open items only.
//
// Only metrics that buildChartQuery averages belong here: a sum is not moved
// by a row of zeros.
var sampleCountColumns = map[string]map[string]string{
	"issue_type_metrics_daily": {"lead_p50_hours": "completed_count"},
}

// averageExpression is buildChartQuery's mean of metric. For a metric with a
// registered sample count the rows with no sample are left out of the mean.
// The NULL branch (not avgIf) keeps a group with no sample NULL, which
// executeChart drops as "no data point"; avgIf would return NaN.
func averageExpression(table, metric string) string {
	if count, ok := sampleCountColumns[table][metric]; ok {
		return fmt.Sprintf("avg(if(%s > 0, %s, NULL))", count, metric)
	}
	return fmt.Sprintf("avg(%s)", metric)
}

// dedupFromSource returns the FROM source for a registry table: its newest
// version of each declared key. A table with no declaration is not readable
// (tableReads, validateChartMetrics refuses it before any query), so there is
// no raw fallback.
func dedupFromSource(table string) string {
	read, ok := tableReads[table]
	if !ok {
		return table
	}
	return fmt.Sprintf(
		"(SELECT * FROM %s ORDER BY %s DESC LIMIT 1 BY %s) AS %s",
		table, read.Version, strings.Join(read.Key, ", "), table,
	)
}
