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

// teamSourceAlias is the alias of the picked rows inside TeamSource.
const teamSourceAlias = "team_view"

// TeamSource is the FROM source of a reader that answers for one team: one row
// per key without row_view, (org_id, day, provider, work_scope_id, team_id[,
// status]), and only live rows. Of the newest row of each view of a key (the
// table read with FINAL) it picks, in one LIMIT 1 BY, a live row first, then the
// team row, then the org row, then the legacy row. A day that no writer of team
// rows computed reads its org rows (or its legacy rows), as the org form does;
// a retracted team row (a row of zeros) never hides a live org row. The live-row
// rule runs again after the pick, so a key whose views are all rows of zeros
// leaves the result.
//
// where is the reader's filter of the table. It runs before the pick, so it
// must name key columns only (org_id, day, provider, work_scope_id, team_id,
// status): a filter on a measure would change which view a key reads. Columns
// in where are qualified by the table name or unqualified. The source has no
// alias of its own: the reader gives it one.
//
// It panics for a table that holds no team rows and for an empty filter: a
// reader of one team must not read every organization.
func TeamSource(table, where string) string {
	if _, ok := teamTables[table]; !ok {
		panic("rowview: table " + table + " holds no team rows")
	}
	if strings.TrimSpace(where) == "" {
		panic("rowview: TeamSource of " + table + " needs a filter")
	}
	declared, ok := teamkeytables.ByTable(table)
	if !ok {
		panic("rowview: table " + table + " is not declared in teamkeytables")
	}
	key := []string{table + ".org_id", table + "." + declared.DayColumn}
	for _, column := range declared.Keys {
		if column.Name != Column {
			key = append(key, table+"."+column.Name)
		}
	}
	view := table + "." + Column
	return fmt.Sprintf(`(
    SELECT * FROM (
        SELECT * FROM %[1]s FINAL
        WHERE (%[2]s)
          AND %[3]s IN ('%[4]s', '%[5]s', '%[6]s')
        ORDER BY %[7]s DESC, multiIf(%[3]s = '%[6]s', 2, %[3]s = '%[5]s', 1, 0) DESC, %[1]s.computed_at DESC
        LIMIT 1 BY %[8]s
    ) AS %[9]s
    WHERE %[10]s
)`, table, where, view, teamkeytables.LegacyRowView, teamkeytables.OrgRowView, teamkeytables.TeamRowView,
		declared.LiveRow(table+"."), strings.Join(key, ", "), teamSourceAlias, declared.LiveRow(teamSourceAlias+"."))
}
