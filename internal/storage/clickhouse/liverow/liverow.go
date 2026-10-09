// Package liverow is the reader side of the one rule for "is this stored row
// of a daily metrics table a measurement": the newest row of a key that holds
// no measure is a retraction row, not a measurement.
//
// The daily tables are append only and their readers take the newest row of a
// key. A key that a later compute no longer produces (a team id that was
// retired for a provider-keyed id, a scope that lost its last item) would keep
// its older row for ever, so the writer stores a row of zeros over it: the key
// and computed_at, and the column default (0 or NULL) in every measure. That
// row adds nothing to a sum. It does change a reader that averages a column
// that is not Nullable, takes a minimum, a maximum or a quantile, counts rows
// or days, or lists the keys of the table: there the stored 0 is the default
// of the column, not a measured 0, and the key is not an entity. Missing is
// not zero, so those readers leave the row out: a retraction row reads exactly
// as an absent row.
//
// Which columns are the measures of a table is declared ONCE, in package
// teamkeytables, from which the writer builds the row of zeros. This package
// holds no second list for those tables: it gives a reader the predicate of
// the registry in the two forms a query needs.
//
// The rule is applied to the NEWEST row of a key, never before the newest row
// is chosen: a filter that ran first would drop the retraction row and serve
// the older row it replaced.
package liverow

import (
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// ownWriterMarkers are the two tables that hold the same rule in their own
// writers and are not in the registry: plain MergeTree tables whose daily
// families (work_item_investment, work_item_issue_type) write a row of zeros
// over a key they no longer produce, and find the live keys of a day by "one
// of these counts of the newest row is not 0"
// (daily.LoadInvestmentMetricsLiveKeys, daily.LoadIssueTypeMetricsLiveKeys).
// The columns here are the columns of those two reads; a test holds them
// against the statements.
var ownWriterMarkers = map[string][]string{
	"investment_metrics_daily": {"delivery_units", "work_items_completed", "prs_merged", "churn_loc"},
	"issue_type_metrics_daily": {"created_count", "completed_count", "active_count"},
}

// noRuleForReaders are the registry tables in which a MEASURED row can hold 0
// or NULL in every measure, so a measured row and a retraction row are equal
// and no reader can drop one and keep the other. A reader of such a table
// gets no predicate here: it must sum, or weight by a count.
var noRuleForReaders = map[string]string{
	"estimate_coverage_metrics_daily": "a group whose items are all closed is stored with a backlog of 0 and no ratio",
	"ai_impact_metrics_daily":         "the bucket 'unknown' is stored for every group, also with no pull request",
}

// Tables returns the tables a reader can apply the rule to, sorted.
func Tables() []string {
	var tables []string
	for _, table := range teamkeytables.All() {
		if _, none := noRuleForReaders[table.Table]; !none {
			tables = append(tables, table.Table)
		}
	}
	for table := range ownWriterMarkers {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

// Registered reports whether a reader can apply the rule to table.
func Registered(table string) bool {
	if _, none := noRuleForReaders[table]; none {
		return false
	}
	if _, own := ownWriterMarkers[table]; own {
		return true
	}
	_, ok := teamkeytables.ByTable(table)
	return ok
}

// Predicate returns the row predicate "this row is a measurement" for a source
// that already holds one row per key: the table read with FINAL, or a subquery
// that selected the newest row and kept its columns. qualifier is the alias
// the source has in the FROM clause, or "" when it is read under the table
// name.
//
// Every column is qualified. A reader often gives an aggregate the name of
// the column it reads (sum(items_completed) AS items_completed), and an
// unqualified name in WHERE or HAVING would then mean that aggregate.
//
// It panics for a table with no rule: a reader that asks for the rule of such
// a table must not silently run without one.
func Predicate(table, qualifier string) string {
	prefix := qualifierPrefix(table, qualifier)
	if columns, own := ownWriterMarkers[table]; own {
		parts := make([]string, len(columns))
		for i, column := range columns {
			parts[i] = prefix + column + " != 0"
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}
	return mustTable(table).LiveRow(prefix)
}

// NewestPredicate returns the same rule for a reader that picks the newest row
// of a key with argMax(column, computed_at) over a GROUP BY of the key of the
// raw table: it is true when the newest row by computed_at is a measurement.
// It belongs in the HAVING clause of that GROUP BY, so the key leaves the
// result when its newest row is a retraction. qualifier is as for Predicate.
func NewestPredicate(table, qualifier string) string {
	prefix := qualifierPrefix(table, qualifier)
	if columns, own := ownWriterMarkers[table]; own {
		parts := make([]string, len(columns))
		for i, column := range columns {
			parts[i] = "argMax(" + prefix + column + ", " + prefix + "computed_at) != 0"
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}
	return "(" + mustTable(table).LiveHaving(prefix) + ")"
}

func qualifierPrefix(table, qualifier string) string {
	if qualifier == "" {
		qualifier = table
	}
	return qualifier + "."
}

func mustTable(table string) teamkeytables.Table {
	if reason, none := noRuleForReaders[table]; none {
		panic("liverow: no reader rule for table " + table + ": " + reason)
	}
	declared, ok := teamkeytables.ByTable(table)
	if !ok {
		panic("liverow: no live-row rule for table " + table)
	}
	return declared
}
