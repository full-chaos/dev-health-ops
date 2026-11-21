// Package rowview is the reader side of the view column of the three aggregate
// work-item daily tables (CHAOS-8863): work_item_metrics_daily,
// work_item_state_durations_daily and work_item_user_metrics_daily.
//
// One work item can be stored under two repositories that name two work scopes.
// A row of these tables is an aggregate of a (scope, team) key, so a reader that
// sums the rows of several scopes counts such an item once for each scope. The
// column row_view (migration 141) tells which question a row answers:
//
//   - legacy (the empty string): every row written before the column, and every
//     row of a writer that does not name it. It counts in every view;
//   - org: the items counted once in the organization;
//   - scope: the items counted once in each work scope;
//   - team: the items counted once for each team that owns them
//     (work_item_metrics_daily and work_item_state_durations_daily only).
//
// A reader that sums across work scopes (an organization total, a team set, a
// person) takes OrgPredicate. A reader that reads one work scope, or lists the
// scopes, takes ScopePredicate. A reader that answers for one team takes
// TeamSource. While no writer stores an org, a scope or a team row, each form
// reads what the legacy rows hold.
//
// The package holds SQL text only. It imports no store and no job package.
package rowview

import (
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// Column is the name of the view column.
const Column = "row_view"

var tables = map[string]struct{}{
	"work_item_metrics_daily":         {},
	"work_item_state_durations_daily": {},
	"work_item_user_metrics_daily":    {},
}

// Has reports whether table holds the view column.
func Has(table string) bool {
	_, ok := tables[table]
	return ok
}

// Tables returns the tables that hold the view column, sorted.
func Tables() []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// OrgPredicate is the row predicate of a reader that sums across work scopes:
// the legacy rows and the rows of the org view. qualifier is the alias of the
// table in the FROM clause, or "" when the table is read under its own name, as
// in liverow.Predicate. The column is always qualified: a reader often gives an
// aggregate the name of a column.
func OrgPredicate(table, qualifier string) string {
	return predicate(table, qualifier, teamkeytables.OrgRowView)
}

// ScopePredicate is the row predicate of a reader of one work scope: the legacy
// rows and the rows of the scope view. qualifier is as for OrgPredicate.
func ScopePredicate(table, qualifier string) string {
	return predicate(table, qualifier, teamkeytables.ScopeRowView)
}

// predicate panics for a table without the column: a reader that asks for the
// view of such a table must not run silently without one.
func predicate(table, qualifier, view string) string {
	if _, ok := tables[table]; !ok {
		panic("rowview: table " + table + " holds no view column")
	}
	if qualifier == "" {
		qualifier = table
	}
	return fmt.Sprintf("(%s.%s IN ('%s', '%s'))", qualifier, Column, teamkeytables.LegacyRowView, view)
}

// teamTables are the tables whose writers store team rows.
var teamTables = map[string]struct{}{
	"work_item_metrics_daily":         {},
	"work_item_state_durations_daily": {},
}

// HasTeamRows reports whether table holds team rows.
func HasTeamRows(table string) bool {
	_, ok := teamTables[table]
	return ok
}

// teamSourceAlias is the alias of the picked rows inside TeamSource.
const teamSourceAlias = "team_view"

// TeamSource is the FROM source of a reader that answers for one team: one row
// per key without row_view, (org_id, day, provider, work_scope_id, team_id[,
// status]), and only live rows. Of the newest row of each view of a key (the
// first row of each full key by order DESC) it picks, in one LIMIT 1 BY, a live row first, then the
// team row, then the org row, then the legacy row (TeamPick). A day that no
// writer of team rows computed reads its org rows (or its legacy rows), as the
// org form does; a retracted team row (a row of zeros) never hides a live org
// row. The live-row rule runs again after the pick, so a key whose views are all
// rows of zeros leaves the result.
//
// where is the reader's filter of the table. It runs before the pick, so it
// must name key columns only (org_id, day, provider, work_scope_id, team_id,
// status): a filter on a measure would change which view a key reads. Columns
// in where are qualified by the table name or unqualified. The source has no
// alias of its own: the reader gives it one.
//
// order is the reader's version order (an expression, the largest wins), as for
// servedNewest; it is never "": the table is not read FINAL, so of two versions
// that share computed_at the order picks one, not the merge order (CHAOS-14407).
//
// It panics for a table that holds no team rows, for an empty filter (a reader
// of one team must not read every organization) and for an empty order.
func TeamSource(table, where, order string) string {
	declared := mustTeamTable(table)
	if strings.TrimSpace(where) == "" {
		panic("rowview: TeamSource of " + table + " needs a filter")
	}
	if strings.TrimSpace(order) == "" {
		panic("rowview: TeamSource of " + table + " needs a version order")
	}
	return fmt.Sprintf("(\n    SELECT * FROM %s AS %s\n    WHERE %s\n)",
		TeamPick(table, orderedNewest(declared, "("+where+")", order)), teamSourceAlias, declared.LiveRow(teamSourceAlias+"."))
}

// TeamPick is the pick of TeamSource without its live-row rule after the pick,
// for a reader that collapses the table itself: newest must be a FROM source
// that holds the newest row of each full key of table, row_view included, and
// that is aliased as the table. Of those rows it keeps one per key without
// row_view, in one LIMIT 1 BY: a live row first, then team, org, legacy, then
// the newest computed_at. A picked row can be a row of zeros (every view of the
// key is retracted); the reader applies the live-row rule where it needs it. The
// result is a parenthesized subquery with no alias.
func TeamPick(table, newest string) string {
	declared := mustTeamTable(table)
	key := []string{table + ".org_id", table + "." + declared.DayColumn}
	for _, column := range declared.Keys {
		if column.Name != Column {
			key = append(key, table+"."+column.Name)
		}
	}
	view := table + "." + Column
	return "(\n        SELECT * FROM " + newest + fmt.Sprintf(`
        WHERE %[1]s IN ('%[2]s', '%[3]s', '%[4]s')
        ORDER BY %[5]s DESC, multiIf(%[1]s = '%[4]s', 2, %[1]s = '%[3]s', 1, 0) DESC, %[6]s.computed_at DESC
        LIMIT 1 BY %[7]s
    )`, view, teamkeytables.LegacyRowView, teamkeytables.OrgRowView, teamkeytables.TeamRowView,
		declared.LiveRow(table+"."), table, strings.Join(key, ", "))
}

// ServedTeamSource is TeamSource for a served (query API) reader (CHAOS-14376):
// the newest rows it picks from are read with the organization's own predicate,
// org_id = {org_id:String}, in this text, so the read is scoped by the helper and
// not by its caller. The caller binds org_id, as every served read does. where is
// the reader's filter of key columns; it panics for a table that holds no team rows
// and for an empty filter. A served reader never takes TeamSource or TeamPick: the
// org scope census refuses them in the served roots. order is as for servedNewest.
func ServedTeamSource(table, where, order string) string {
	declared := mustTeamTable(table)
	return fmt.Sprintf("(\n    SELECT * FROM %s AS %s\n    WHERE %s\n)",
		TeamPick(table, servedNewest(declared, where, order)), teamSourceAlias, declared.LiveRow(teamSourceAlias+"."))
}

// ServedTeamPick is TeamPick for a served reader of each key's newest day: the
// same organization-scoped read as ServedTeamSource, and no live-row rule after
// the pick, so a key whose every view is retracted keeps its row of zeros as its
// newest row (it adds 0 and keeps that day the key's newest day, as the org form
// does). It panics as ServedTeamSource does. order is as for servedNewest.
func ServedTeamPick(table, where, order string) string {
	return TeamPick(table, servedNewest(mustTeamTable(table), where, order))
}

// servedNewest is the newest row of each full key of the table in the organization,
// aliased as the table. With order "" the table is read FINAL, so of two versions
// that share computed_at the merge order picks one. Else order is the reader's
// version order (an expression, the largest wins): each full key keeps its first
// row by order DESC, so the reader's org form, which picks by the same order,
// serves the same version (CHAOS-14405).
func servedNewest(declared teamkeytables.Table, where, order string) string {
	table := declared.Table
	if strings.TrimSpace(where) == "" {
		panic("rowview: a served team view of " + table + " needs a filter")
	}
	if order == "" {
		return fmt.Sprintf("(SELECT * FROM %[1]s FINAL WHERE %[1]s.org_id = {org_id:String} AND (%[2]s)) AS %[1]s", table, where)
	}
	return orderedNewest(declared, table+".org_id = {org_id:String} AND ("+where+")", order)
}

// orderedNewest is the first row of each full key of the table (row_view
// included) by order DESC among the rows that pass filter, aliased as the table.
func orderedNewest(declared teamkeytables.Table, filter, order string) string {
	table := declared.Table
	key := []string{table + ".org_id", table + "." + declared.DayColumn}
	for _, column := range declared.Keys {
		key = append(key, table+"."+column.Name)
	}
	return fmt.Sprintf("(SELECT * FROM %[1]s WHERE %[2]s ORDER BY %[3]s DESC LIMIT 1 BY %[4]s) AS %[1]s",
		table, filter, order, strings.Join(key, ", "))
}

// WorkItemMetricsNewestOrder is the version order of a work_item_metrics_daily key, for the `order` argument of
// ServedTeamSource / ServedTeamPick and a reader's own pick: the newest computed_at, then the highest hash of the
// version's 19 measures, so two versions that share a computed_at serve one of them whatever the insert order. Every
// served reader of the table that picks a version takes this one order, so the pages serve the same version of a tied
// key.
const WorkItemMetricsNewestOrder = `(work_item_metrics_daily.computed_at, cityHash64(tuple(
    work_item_metrics_daily.items_started, work_item_metrics_daily.items_completed,
    work_item_metrics_daily.items_started_unassigned, work_item_metrics_daily.items_completed_unassigned,
    work_item_metrics_daily.wip_count_end_of_day, work_item_metrics_daily.wip_unassigned_end_of_day,
    work_item_metrics_daily.bug_completed_ratio, work_item_metrics_daily.story_points_completed,
    work_item_metrics_daily.new_bugs_count, work_item_metrics_daily.new_items_count,
    work_item_metrics_daily.defect_intro_rate, work_item_metrics_daily.wip_congestion_ratio,
    work_item_metrics_daily.predictability_score, work_item_metrics_daily.cycle_time_p50_hours,
    work_item_metrics_daily.cycle_time_p90_hours, work_item_metrics_daily.lead_time_p50_hours,
    work_item_metrics_daily.lead_time_p90_hours, work_item_metrics_daily.wip_age_p50_hours,
    work_item_metrics_daily.wip_age_p90_hours)))`

// StateDurationsNewestOrder is the version order of a work_item_state_durations_daily key, as
// WorkItemMetricsNewestOrder is for work_item_metrics_daily: the newest computed_at, then the highest hash of the
// version's three measures.
const StateDurationsNewestOrder = `(work_item_state_durations_daily.computed_at, cityHash64(tuple(
                work_item_state_durations_daily.duration_hours, work_item_state_durations_daily.items_touched,
                work_item_state_durations_daily.avg_wip)))`

// versionColumns are the stored columns of a repository-keyed table, every column but the key and computed_at, in
// table order. Every metric of the table hashes this one list, so all metrics of a response read one stored version
// of a tied key. A cell holds each list equal to the table's columns at the migration head.
var versionColumns = map[string]string{
	"repo_metrics_daily": `commits_count total_loc_touched avg_commit_size_loc large_commit_ratio prs_merged
		median_pr_cycle_hours pr_cycle_p75_hours pr_cycle_p90_hours prs_with_first_review pr_first_review_p50_hours
		pr_first_review_p90_hours pr_review_time_p50_hours pr_pickup_time_p50_hours large_pr_ratio pr_rework_ratio
		mttr_hours change_failure_rate pr_size_p50_loc pr_size_p90_loc pr_comments_per_100_loc pr_reviews_per_100_loc
		rework_churn_ratio_30d single_owner_file_ratio_30d review_load_top_reviewer_ratio bus_factor code_ownership_gini
		change_failure_rate_incident revert_rate prs_merged_reviewed prs_merged_rework prs_merged_no_rework_signal
		pr_rework_ratio_reviewed pooled_pr_size_loc pooled_bug_repair_hours pooled_samples_stored retracted`,
	"deploy_metrics_daily": `deployments_count failed_deployments_count deploy_time_p50_hours lead_time_p50_hours
		pooled_deploy_hours pooled_samples_stored retracted`,
	"repo_change_failure_daily": `deployments_count failed_deployments_native failed_deployments_heuristic
		incidents_direct incidents_via_deployment failed_deployments_status deployments_status_known`,
	"testops_pipeline_metrics_daily": `pipelines_count success_count failure_count cancelled_count success_rate
		failure_rate cancel_rate rerun_rate median_duration_seconds p95_duration_seconds avg_queue_seconds
		p95_queue_seconds team_id service_id skipped_count unfinished_count other_count unfinished_stale_count
		pooled_service_ids pooled_durations pooled_queues pooled_samples_stored retracted`,
}

// VersionColumns is the fixed hash list of a repository-keyed table (nil for another table).
func VersionColumns(table string) []string {
	return strings.Fields(versionColumns[table])
}

// VersionOrder is the version order of a key of table: the shared order of a team table, else the newest
// computed_at, then the highest hash of the table's fixed column list, the same for every metric of the table. It
// panics for a table with no order. Columns are qualified by the table, so the order reads the stored row and not an
// alias of the select list.
func VersionOrder(table string) string {
	switch table {
	case "work_item_metrics_daily":
		return WorkItemMetricsNewestOrder
	case "work_item_state_durations_daily":
		return StateDurationsNewestOrder
	}
	if _, ok := versionColumns[table]; !ok {
		panic("rowview: no version order for " + table)
	}
	return "(" + table + ".computed_at, cityHash64(tuple(" + qualified(table, VersionColumns(table)) + ")))"
}

// VersionPick is the select list that picks ONE version of each key by VersionOrder: the tuple of the columns as
// picked, then picked.N AS <column> for each. A Nullable column keeps the picked version's NULL (a tuple is never
// NULL), so no column comes from another version. The caller groups by the key.
func VersionPick(table string, columns []string) string {
	items := []string{"argMax(tuple(" + qualified(table, columns) + "), " + VersionOrder(table) + ") AS picked"}
	for i, column := range columns {
		items = append(items, fmt.Sprintf("picked.%d AS %s", i+1, column))
	}
	return strings.Join(items, ",\n                ")
}

func qualified(table string, columns []string) string {
	out := make([]string, len(columns))
	for i, column := range columns {
		out[i] = table + "." + column
	}
	return strings.Join(out, ", ")
}

// mustTeamTable panics for a table that holds no team rows.
func mustTeamTable(table string) teamkeytables.Table {
	if _, ok := teamTables[table]; !ok {
		panic("rowview: table " + table + " holds no team rows")
	}
	declared, ok := teamkeytables.ByTable(table)
	if !ok {
		panic("rowview: table " + table + " is not declared in teamkeytables")
	}
	return declared
}
