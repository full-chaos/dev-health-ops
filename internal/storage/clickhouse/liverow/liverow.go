// Package liverow holds the one rule for "is this stored row of a daily
// metrics table a measurement": a row whose count columns are all zero is a
// retraction row, not a measurement.
//
// The daily tables are append only and their readers take the newest row of a
// key. A key that a later compute no longer produces (a team id that was
// retired for a provider-keyed id, a scope that lost its last item) would keep
// its older row for ever, so the writer stores a row of zeros over it: every
// count column 0, every Nullable measure NULL. That row adds nothing to a sum.
// It does change a reader that averages a column that is not Nullable, takes a
// minimum, a maximum or a quantile, counts rows or days, or lists the keys of
// the table: there the stored 0 is the default of the column, not a measured
// 0, and the key is not an entity. Missing is not zero, so those readers leave
// the row out.
//
// A measured row always has a count above zero (a row exists only because an
// item, a commit or a pull request was counted for it), so the rule drops no
// measurement.
//
// The rule is applied to the NEWEST row of a key, never before the newest row
// is chosen: a filter that ran first would drop the retraction row and serve
// the older row it replaced.
package liverow

import (
	"sort"
	"strings"
)

// markerColumns names, per daily table, the columns of which a measured row
// always holds one that is not zero. They are the count columns of the row.
// compounding_risk_daily has no count: its score is NULL both for a team that
// was measured with too little data and for a retraction row, so its marker is
// the configuration (weights and thresholds) every measured row stores and a
// retraction row does not.
var markerColumns = map[string][]string{
	"work_item_metrics_daily": {
		"items_started", "items_completed", "items_started_unassigned", "items_completed_unassigned",
		"wip_count_end_of_day", "wip_unassigned_end_of_day", "new_bugs_count", "new_items_count",
	},
	"work_item_state_durations_daily": {"items_touched"},
	"estimate_coverage_metrics_daily": {"estimated_count", "unestimated_count", "backlog_size"},
	"team_metrics_daily":              {"commits_count", "after_hours_commits_count", "weekend_commits_count"},
	"investment_metrics_daily":        {"delivery_units", "work_items_completed", "prs_merged", "churn_loc"},
	"issue_type_metrics_daily":        {"created_count", "completed_count", "active_count"},
	"ai_impact_metrics_daily": {
		"prs_total", "prs_merged", "ai_assisted_prs", "agent_created_prs", "human_prs", "unknown_prs",
		"agent_created_pr_count", "rework_prs", "followup_commits_count", "revert_prs", "incidents_count",
		"test_gap_prs",
	},
	"ai_governance_coverage_daily": {
		"ai_artifacts", "declared_artifacts", "human_reviewed_prs", "security_scanned_prs", "in_policy_artifacts",
	},
	"compounding_risk_daily": {
		"w_churn", "w_complexity", "w_ownership", "w_review", "threshold_elevated", "threshold_high",
	},
}

// Tables returns the registered tables, sorted.
func Tables() []string {
	tables := make([]string, 0, len(markerColumns))
	for table := range markerColumns {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

// MarkerColumns returns the marker columns of table, or nil when the table is
// not registered.
func MarkerColumns(table string) []string {
	return append([]string(nil), markerColumns[table]...)
}

// Registered reports whether table has a live-row rule.
func Registered(table string) bool {
	_, ok := markerColumns[table]
	return ok
}

// Predicate returns the row predicate "this row is a measurement" for a source
// that already holds one row per key: the table read with FINAL, or a subquery
// that selected the newest row. qualifier is the alias the source has in the
// FROM clause, or "" when it is read under the table name.
//
// Every column is qualified. A reader often gives an aggregate the name of
// the column it reads (sum(items_completed) AS items_completed), and an
// unqualified name in WHERE or HAVING would then mean that aggregate.
//
// It panics for a table that is not registered: a reader that asks for the
// rule of an unknown table has no rule, and must not silently run without one.
func Predicate(table, qualifier string) string {
	columns := mustColumns(table)
	if qualifier == "" {
		qualifier = table
	}
	parts := make([]string, len(columns))
	for i, column := range columns {
		parts[i] = qualifier + "." + column + " != 0"
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// NewestPredicate returns the same rule for a reader that picks the newest row
// of a key with argMax(column, computed_at) over a GROUP BY of the key: it is
// true when the newest row by computed_at is a measurement. It belongs in the
// HAVING clause of that GROUP BY, so the key leaves the result when its
// newest row is a retraction. qualifier is as for Predicate.
func NewestPredicate(table, qualifier string) string {
	if qualifier == "" {
		qualifier = table
	}
	return "argMax(" + Predicate(table, qualifier) + ", " + qualifier + ".computed_at)"
}

// Measured reports whether a row already read into Go is a measurement. It is
// for a reader whose SQL text is pinned and cannot carry the predicate.
func Measured(counts ...uint64) bool {
	for _, count := range counts {
		if count != 0 {
			return true
		}
	}
	return false
}

func mustColumns(table string) []string {
	columns, ok := markerColumns[table]
	if !ok {
		panic("liverow: no live-row rule for table " + table)
	}
	return columns
}
