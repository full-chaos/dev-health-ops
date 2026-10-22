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
//   - scope: the items counted once in each work scope.
//
// A reader that sums across work scopes (an organization total, a team set, a
// person) takes OrgPredicate. A reader that reads one work scope, or lists the
// scopes, takes ScopePredicate. While no writer stores an org or a scope row,
// both predicates keep every row.
//
// The package holds SQL text only. It imports no store and no job package.
package rowview

import (
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// Column is the name of the view column.
const Column = "row_view"

var tables = map[string]struct{}{
	"work_item_metrics_daily":         {},
	"work_item_state_durations_daily": {},
	"work_item_user_metrics_daily":    {},
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
