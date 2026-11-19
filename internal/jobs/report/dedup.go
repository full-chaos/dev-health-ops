package report

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// CHAOS-4246: the weekly-report engine charts daily metric rollups that are
// append-only in practice: a re-run of a day adds a new version of a key
// (computed_at later) and merges only eventually, so a bare sum() or avg()
// counts every version. dedupFromSource reads the newest version of each key
// for EVERY table the registry charts. Which key and which version column each
// table has is declared once, with its date column, in tableReads (tablereads.go).
// This Go declaration does not depend on the Python mirror
// (src/dev_health_ops/clickhouse_dedup.py), which is not consulted here.

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
//
// The pipeline failure rate is the bucket's failures over its completed runs
// (succeeded or failed), from the counts of the metric's own table. The stored
// failure_rate column holds 0 on a day with no completed run, so it is never
// charted. A bucket with no completed run has no point.
//
// The pipeline success rate is the same rule over successes, from the counts of
// testops_pipeline_metrics_daily, as Pipelines, Home and Risk read them. The
// CI/CD column cicd_metrics_daily.success_rate divided by every run (cancelled,
// skipped and running included) and held 0 on a day with no run counted; it is
// no longer charted.
//
// The test pass rate is the bucket's passed cases over its passed and failed
// cases, from the counts of testops_test_metrics_daily, as the TestOps Tests
// page reads them. The stored pass_rate column divides by every case (skipped
// ones too) and holds 0 on a day where every case was skipped, so it is never
// charted. A bucket with no passed or failed case has no point.
//
// The test flake rate and retry dependency rate are the bucket's flake (retry-dependent) cases over its distinct cases,
// from the counts of testops_test_metrics_daily. A mean of the stored repository-day rates weighs a day of 1 case like a
// day of 500. A repository-day with a NULL rate or NULL counts is in neither sum, and a bucket made only of such days
// has no point. The failure recurrence score has no stored count and is still the mean of the stored values.
var chartRules = map[string]chartRule{
	"change_failure_rate":   {table: changefailure.Table, expression: changefailure.WindowRateSQL, inputs: changefailure.CountColumns},
	"pr_rework_ratio":       {table: prrework.Table, expression: prrework.WindowRateSQL, inputs: prrework.CountColumns},
	"failure_rate":          {table: "testops_pipeline_metrics_daily", expression: testops.PipelineFailureRateWindowSQL, inputs: testops.PipelineRateCountColumns},
	"success_rate":          {table: "testops_pipeline_metrics_daily", expression: testops.PipelineSuccessRateWindowSQL, inputs: testops.PipelineRateCountColumns},
	"pass_rate":             {table: "testops_test_metrics_daily", expression: testops.TestPassRateWindowSQL, inputs: testops.TestRateCountColumns},
	"flake_rate":            {table: "testops_test_metrics_daily", expression: testops.TestFlakeRateWindowSQL, inputs: testops.TestFlakeRateCountColumns},
	"retry_dependency_rate": {table: "testops_test_metrics_daily", expression: testops.TestRetryDependencyRateWindowSQL, inputs: testops.TestRetryDependencyRateCountColumns},
}

// withChartRule returns the definition a chart of the metric is built from:
// the registry definition, on the rule's table when the metric has a rule.
func withChartRule(definition metricDefinition) metricDefinition {
	if rule, ok := chartRules[definition.CanonicalName]; ok &&
		(definition.SourceTable == "repo_metrics_daily" || definition.SourceTable == rule.table) {
		definition.SourceTable = rule.table
		definition.rule = rule.expression
	}
	return definition
}

// dedupFromSource returns the FROM source for a registry table: its newest
// version of each declared key. A table with no declaration is not readable
// (tableReads, validateChartMetrics refuses it before any query), so there is
// no raw fallback.
//
// For a table with a live-row rule (package liverow) the source also leaves
// out the retraction rows: the newest row of a key the compute no longer
// produces (a retired team id) holds no measure and is not a measurement.
// buildChartQuery averages most metrics, so each such row would be one more
// sample of 0, and a chart grouped by team would show the retired id as a
// team. The rule is applied to the rows that LIMIT 1 BY kept, never before: a
// filter that ran first would drop the retraction row and serve the older row
// it replaced.
func dedupFromSource(table string) string {
	read, ok := tableReads[table]
	if !ok {
		return table
	}
	newest := fmt.Sprintf(
		"(SELECT * FROM %s ORDER BY %s DESC LIMIT 1 BY %s) AS %s",
		table, read.Version, strings.Join(read.Key, ", "), table,
	)
	if table == "work_item_cycle_times" {
		// A reopened item's newest row is a retraction: no completed_at, no hours. It is not a completion.
		return fmt.Sprintf("(SELECT * FROM %s WHERE completed_at IS NOT NULL) AS %s", newest, table)
	}
	if !liverow.Registered(table) {
		return newest
	}
	conditions := liverow.Predicate(table, "")
	if rowview.Has(table) {
		// Each item once in the organization (CHAOS-8863): the legacy rows and the org view. row_view is in the key
		// above, so a legacy retraction row never replaces an org row of the same (scope, team, day).
		conditions += " AND " + rowview.OrgPredicate(table, "")
	}
	return fmt.Sprintf("(SELECT * FROM %s WHERE %s) AS %s", newest, conditions, table)
}
