package report

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
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

// chartRule is a chart metric that is not a stored column: its value is a
// rule over the stored counts of another table.
type chartRule struct {
	// table holds the counts; it must be declared in tableReads.
	table string
	// expression aggregates the counts of a chart bucket into the value. NULL
	// means the bucket has no value, and executeChart draws no point.
	expression string
	// inputs are the columns of table that expression reads.
	inputs []string
}

// chartRules names, per registry metric, the rule its chart reads instead of
// the column of the metric's own name.
//
// Change failure rate follows the view (CHAOS-8981): a chart bucket is a
// window, so its value is the shared window rule over the bucket's summed
// counts, the same number Home, /explain and the operating review give for
// that window. An average of stored one-day values is a different number (a
// day whose incident starts on another day has no value of its own), and the
// column repo_metrics_daily.change_failure_rate is DEPRECATED (CHAOS-9017
// drops it): it still holds the legacy revert ratio for older readers and is
// never charted as change failure rate. The exported registry keeps naming
// repo_metrics_daily as the metric's table; the chart reports the table it
// reads.
//
// The pull request rework ratio follows the view too: a chart bucket's value
// is the shared window rule over the bucket's summed counts of REVIEWED pull
// requests (package prrework), on the metric's own table. The column
// repo_metrics_daily.pr_rework_ratio is DEPRECATED: it divides by all merged
// pull requests and holds 0 where nothing was reviewed, so it is never
// charted. A bucket with no reviewed pull request has no point.
var chartRules = map[string]chartRule{
	"change_failure_rate": {table: changefailure.Table, expression: changefailure.WindowRateSQL, inputs: changefailure.CountColumns},
	"pr_rework_ratio":     {table: prrework.Table, expression: prrework.WindowRateSQL, inputs: prrework.CountColumns},
}

// withChartRule returns the definition a chart of the metric is built from:
// the registry definition, on the rule's table when the metric has a rule.
func withChartRule(definition metricDefinition) metricDefinition {
	if rule, ok := chartRules[definition.CanonicalName]; ok && definition.SourceTable == "repo_metrics_daily" {
		definition.SourceTable = rule.table
		definition.rule = rule.expression
	}
	return definition
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
