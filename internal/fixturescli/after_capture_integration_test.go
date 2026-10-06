//go:build integration

package fixturescli

// What a migration adds AFTER the frozen worlds were recorded.
//
// The frozen worlds (testdata/generate, testdata/synthetic) are recordings of
// the Python fixtures generator. They cannot be recorded again: the producer
// cannot run (see frozenWorldDigests). So a migration that changes a table
// the worlds write has no capture to compare with, and the world tests would
// fail on it for ever -- or, worse, be taught to ignore it.
//
// The rules below are the third way, the one serverStampedColumns already
// takes for a server-stamped column: the captures and their pinned digests
// stay as they are, the new thing is NAMED in one list with the migration
// that added it, and it is CHECKED by a Go-only rule on the real database
// after the load. Nothing is ignored:
//
//   - a NULLABLE COLUMN added after the capture (columnsAfterTheCapture): a
//     captured row has no value for it, so the load must leave it NULL. Every
//     loaded row is checked; only then is the column left out of the
//     comparison with the frozen columns.
//   - a table a MATERIALIZED VIEW fills, added after the capture
//     (viewFilledAfterTheCapture): the load does not insert into it, the view
//     does. Its rows are checked against the rule the view states, computed
//     from the rows of the table the view reads: one row per key, and the
//     filled value equal to the rule's value. It is not "a table the load may
//     change"; any table that is NOT in the list and changes still fails.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// columnsAfterTheCapture names every nullable column a migration added, after
// the frozen worlds were recorded, to a table they write: table -> column ->
// the migration that added it. This is the ONE list for such columns.
var columnsAfterTheCapture = map[string]map[string]string{
	// The provider's own time of a work item relation (CHAOS-8574). No
	// frozen relation has one.
	// Which items of a relation write it (CHAOS-8578). No frozen relation
	// has one either.
	"work_item_dependencies": {
		"relation_started_at": "102_work_item_dependency_first_seen.sql",
		"relation_writer":     "103_work_item_relation_writer_and_read.sql",
	},
	// The number of distinct quotes the row's run wrote (CHAOS-8788): the
	// materializer records it; a captured row has no value for it, so the
	// load leaves it NULL ("not recorded" = complete).
	"work_unit_investments": {
		"evidence_quote_count": "106_work_unit_investment_quote_count.sql",
	},
}

// viewFilledTable is one table that a materialized view fills.
type viewFilledTable struct {
	// View is the materialized view that writes the table, Source the table
	// the view reads, Migration the file that added both.
	View, Source, Migration string
	// Key are the columns one row of the table is identified by, in both the
	// table and Source. Column is the filled column, and SourceRule the
	// aggregate over Source's rows of one key that Column must equal.
	Key        []string
	Column     string
	SourceRule string
	// SourceWhere, when set, is the filter of the view: only the Source
	// rows it keeps reach the table, so only they enter the rule.
	SourceWhere string
}

// viewFilledAfterTheCapture names every table a migration added, after the
// frozen worlds were recorded, that a materialized view fills when the
// worlds' rows are inserted. This is the ONE list for such tables.
var viewFilledAfterTheCapture = map[string]viewFilledTable{
	// The first time a sync wrote a work item relation (CHAOS-8574): one row
	// per relation, first_seen_at = the smallest last_synced of the relation.
	"work_item_dependency_first_seen": {
		View: "work_item_dependency_first_seen_mv", Source: "work_item_dependencies",
		Migration:  "102_work_item_dependency_first_seen.sql",
		Key:        []string{"org_id", "source_work_item_id", "target_work_item_id", "relationship_type"},
		Column:     "first_seen_at",
		SourceRule: "min(last_synced)",
	},
	// The latest time a pass that READS an item's relations wrote the item
	// (CHAOS-8578): one row per item, relations_read_at = the largest
	// last_synced of its work_items rows that are not github Projects v2
	// board rows.
	"work_item_relations_read": {
		View: "work_item_relations_read_mv", Source: "work_items",
		Migration:   "103_work_item_relation_writer_and_read.sql",
		Key:         []string{"org_id", "work_item_id"},
		Column:      "relations_read_at",
		SourceRule:  "max(last_synced)",
		SourceWhere: "NOT (provider = 'github' AND startsWith(project_id, 'ghprojv2:'))",
	},
}

// withoutColumnsAfterTheCapture checks the columns of table that
// columnsAfterTheCapture names and returns the table without them, so the
// rest can be compared with the frozen columns. A problem is reported, never
// dropped, when a named column is not in the table (the entry has rotted),
// is not Nullable (a captured row could not leave it NULL), or holds a value
// in any loaded row (the load wrote something no capture holds).
func withoutColumnsAfterTheCapture(table FrozenTable) (FrozenTable, []string) {
	added := columnsAfterTheCapture[table.Name]
	if len(added) == 0 {
		return table, nil
	}
	names := make([]string, 0, len(added))
	for name := range added {
		names = append(names, name)
	}
	sort.Strings(names)
	var problems []string
	drop := map[int]bool{}
	for _, name := range names {
		position := -1
		for index, column := range table.Columns {
			if column.Name == name {
				position = index
			}
		}
		if position < 0 {
			problems = append(problems, fmt.Sprintf("%s.%s (added by %s): the table has no such column; the entry in columnsAfterTheCapture has rotted", table.Name, name, added[name]))
			continue
		}
		drop[position] = true
		if columnType := table.Columns[position].Type; !strings.HasPrefix(columnType, "Nullable(") {
			problems = append(problems, fmt.Sprintf("%s.%s (added by %s) is %s, not Nullable: a captured row has no value for it and could not leave it NULL", table.Name, name, added[name], columnType))
			continue
		}
		valued := 0
		for _, row := range table.Rows {
			if position >= len(row) || row[position] != nil {
				valued++
			}
		}
		if valued > 0 {
			problems = append(problems, fmt.Sprintf("%s.%s (added by %s, after the capture): %d of %d loaded rows hold a value; a captured row has none, so the load must leave it NULL",
				table.Name, name, added[name], valued, len(table.Rows)))
		}
	}
	out := FrozenTable{Name: table.Name, Rows: make([][]any, len(table.Rows))}
	for index, column := range table.Columns {
		if !drop[index] {
			out.Columns = append(out.Columns, column)
		}
	}
	for rowIndex, row := range table.Rows {
		projected := make([]any, 0, len(row))
		for index, value := range row {
			if !drop[index] {
				projected = append(projected, value)
			}
		}
		out.Rows[rowIndex] = projected
	}
	return out, problems
}

// requireColumnsAfterTheCapture is withoutColumnsAfterTheCapture for a dump
// in a test: any problem fails the test.
func requireColumnsAfterTheCapture(t *testing.T, table FrozenTable) FrozenTable {
	t.Helper()
	out, problems := withoutColumnsAfterTheCapture(table)
	if len(problems) > 0 {
		t.Fatalf("columns added after the capture:\n  %s", strings.Join(problems, "\n  "))
	}
	return out
}

// viewFilledRowProblems compares what a view-filled table holds with what
// its rule gives, both as key -> value. A key the rule gives and the table
// lacks is a view that wrote no row; a key the table holds and the rule does
// not give is a row from nowhere; a differing value is a view that wrote the
// wrong value. Each is named with its key.
func viewFilledRowProblems(table string, rule viewFilledTable, want, got map[string]string) []string {
	var problems []string
	keys := make([]string, 0, len(want)+len(got))
	seen := map[string]bool{}
	for key := range want {
		keys, seen[key] = append(keys, key), true
	}
	for key := range got {
		if !seen[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		wanted, inWant := want[key]
		held, inGot := got[key]
		switch {
		case !inGot:
			problems = append(problems, fmt.Sprintf("%s (filled by %s, migration %s): no row for %s; %s has that key, so the view wrote no row for it", table, rule.View, rule.Migration, key, rule.Source))
		case !inWant:
			problems = append(problems, fmt.Sprintf("%s (filled by %s, migration %s): a row for %s, which %s does not hold", table, rule.View, rule.Migration, key, rule.Source))
		case wanted != held:
			problems = append(problems, fmt.Sprintf("%s (filled by %s, migration %s): %s of %s is %s, want %s = %s over the %s rows of that key", table, rule.View, rule.Migration, rule.Column, key, held, rule.SourceRule, wanted, rule.Source))
		}
	}
	return problems
}

// keyedValues parses a TSV body of key columns and one value column into
// key -> value. The key is the key columns joined by a tab.
func keyedValues(body string, keyColumns int) map[string]string {
	values := map[string]string{}
	body = strings.TrimRight(body, "\n")
	if body == "" {
		return values
	}
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != keyColumns+1 {
			values[line] = "<a row that is not " + fmt.Sprint(keyColumns) + " key columns and one value>"
			continue
		}
		values[strings.Join(fields[:keyColumns], "\t")] = fields[keyColumns]
	}
	return values
}

// viewFilledProblems checks every table of viewFilledAfterTheCapture on the
// live database: its view exists, and its rows are what its rule gives over
// the rows of the table the view reads. The table's own value is read as the
// aggregate over the key (an AggregatingMergeTree holds one row per insert
// block until parts merge), which is how every reader of it must read it.
func viewFilledProblems(t *testing.T, dsn string) []string {
	t.Helper()
	names := make([]string, 0, len(viewFilledAfterTheCapture))
	for name := range viewFilledAfterTheCapture {
		names = append(names, name)
	}
	sort.Strings(names)
	var problems []string
	for _, name := range names {
		problems = append(problems, viewFilledTableProblems(t, dsn, name, viewFilledAfterTheCapture[name])...)
	}
	return problems
}

// viewFilledTableProblems checks one view-filled table against its rule on
// the live database (viewFilledProblems).
func viewFilledTableProblems(t *testing.T, dsn, name string, rule viewFilledTable) []string {
	t.Helper()
	var problems []string
	if views := strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '"+rule.View+"' AND engine = 'MaterializedView' FORMAT TSV")); views != "1" {
		problems = append(problems, fmt.Sprintf("%s (migration %s): the materialized view %s does not exist; nothing fills the table", name, rule.Migration, rule.View))
	}
	wantQuery, gotQuery := viewFilledQueries(name, rule)
	want := keyedValues(clickHouseHTTP(t, dsn, wantQuery), len(rule.Key))
	got := keyedValues(clickHouseHTTP(t, dsn, gotQuery), len(rule.Key))
	return append(problems, viewFilledRowProblems(name, rule, want, got)...)
}

// viewFilledQueries are the two reads of the check: what the rule gives over
// the source rows the view keeps (its SourceWhere), and what the table holds,
// each as the aggregate over the key.
func viewFilledQueries(name string, rule viewFilledTable) (want, got string) {
	keys := "`" + strings.Join(rule.Key, "`, `") + "`"
	aggregate := rule.SourceRule[:strings.Index(rule.SourceRule, "(")]
	where := ""
	if rule.SourceWhere != "" {
		where = " WHERE " + rule.SourceWhere
	}
	want = fmt.Sprintf("SELECT %s, toString(%s) FROM `%s`%s GROUP BY %s ORDER BY %s FORMAT TSV", keys, rule.SourceRule, rule.Source, where, keys, keys)
	got = fmt.Sprintf("SELECT %s, toString(%s(`%s`)) FROM `%s` GROUP BY %s ORDER BY %s FORMAT TSV", keys, aggregate, rule.Column, name, keys, keys)
	return want, got
}

// requireViewFilledTables fails the test on any problem of a view-filled
// table added after the capture.
func requireViewFilledTables(t *testing.T, dsn string) {
	t.Helper()
	if problems := viewFilledProblems(t, dsn); len(problems) > 0 {
		t.Fatalf("tables a view fills, added after the capture:\n  %s", strings.Join(problems, "\n  "))
	}
}

// unwrittenTableProblems lists every table whose row count changed in a load
// that does not write it. wrote names the tables the loaded world (or
// target) holds. A table of viewFilledAfterTheCapture is not reported when
// the load wrote the table its view reads -- its rows are held to their rule
// by viewFilledProblems instead -- and IS reported when the load did not:
// nothing then should have filled it. Every other table is reported.
func unwrittenTableProblems(before, after map[string]string, wrote map[string]bool) []string {
	names := make([]string, 0, len(after))
	for name := range after {
		names = append(names, name)
	}
	sort.Strings(names)
	var problems []string
	for _, name := range names {
		if wrote[name] || after[name] == before[name] {
			continue
		}
		if rule, viewFilled := viewFilledAfterTheCapture[name]; viewFilled && wrote[rule.Source] {
			continue
		}
		problems = append(problems, fmt.Sprintf("the load changed %s (%s -> %s), which it does not write", name, before[name], after[name]))
	}
	return problems
}

// The rules, with no database: each case is one defect a load or a view can
// have, and each is reported by name.
func TestAfterCaptureRulesReportEveryDefectByName(t *testing.T) {
	// ---- a nullable column added after the capture ----
	columns := []FrozenColumn{
		{Name: "source_work_item_id", Type: "String"},
		{Name: "relation_started_at", Type: "Nullable(DateTime64(3))"},
		{Name: "relation_writer", Type: "Nullable(String)"},
		{Name: "org_id", Type: "String"},
	}
	clean := FrozenTable{Name: "work_item_dependencies", Columns: columns, Rows: [][]any{{"a", nil, nil, "o"}, {"b", nil, nil, "o"}}}
	out, problems := withoutColumnsAfterTheCapture(clean)
	if len(problems) != 0 {
		t.Fatalf("every row NULL: %v", problems)
	}
	if len(out.Columns) != 2 || out.Columns[0].Name != "source_work_item_id" || out.Columns[1].Name != "org_id" ||
		len(out.Rows) != 2 || len(out.Rows[0]) != 2 || out.Rows[0][0] != "a" || out.Rows[0][1] != "o" || out.Rows[1][0] != "b" {
		t.Fatalf("the table without the added column = %+v", out)
	}
	if len(clean.Columns) != 4 || len(clean.Rows[0]) != 4 {
		t.Fatal("the input table was edited")
	}
	// PLANT 1: a load that writes a value into the new column.
	written := FrozenTable{Name: "work_item_dependencies", Columns: columns, Rows: [][]any{{"a", nil, nil, "o"}, {"b", "2026-08-01 00:00:00.000", nil, "o"}}}
	if _, problems := withoutColumnsAfterTheCapture(written); len(problems) != 1 ||
		!strings.HasPrefix(problems[0], "work_item_dependencies.relation_started_at (added by 102_work_item_dependency_first_seen.sql, after the capture): 1 of 2 loaded rows hold a value") {
		t.Fatalf("a value in the added column: %q", problems)
	}
	// The entry outlived its column, or the column is not nullable.
	gone := FrozenTable{Name: "work_item_dependencies", Columns: columns[:1], Rows: [][]any{{"a"}}}
	if _, problems := withoutColumnsAfterTheCapture(gone); len(problems) != 2 || !strings.Contains(problems[0], "the table has no such column") || !strings.Contains(problems[1], "the table has no such column") {
		t.Fatalf("two named columns that are not in the table: %q", problems)
	}
	notNullable := FrozenTable{Name: "work_item_dependencies", Columns: []FrozenColumn{{Name: "relation_started_at", Type: "DateTime64(3)"}, {Name: "relation_writer", Type: "Nullable(String)"}}, Rows: [][]any{{"1970-01-01 00:00:00.000", nil}}}
	if _, problems := withoutColumnsAfterTheCapture(notNullable); len(problems) != 1 || !strings.Contains(problems[0], "not Nullable") {
		t.Fatalf("a named column that is not nullable: %q", problems)
	}
	// A table with no added column is returned as it is.
	other := FrozenTable{Name: "repos", Columns: columns, Rows: [][]any{{"a", "value", "x", "o"}}}
	if out, problems := withoutColumnsAfterTheCapture(other); len(problems) != 0 || len(out.Columns) != 4 {
		t.Fatalf("a table that is not in the list: %+v %v", out, problems)
	}

	// ---- a table a view fills, added after the capture ----
	rule := viewFilledAfterTheCapture["work_item_dependency_first_seen"]
	if rule.View == "" || rule.Source == "" || len(rule.Key) == 0 {
		t.Fatal("the first-seen table is not in viewFilledAfterTheCapture: the rules below would hold nothing")
	}
	want := keyedValues("o\ta\tb\tblocks\t2026-08-01 10:00:00.000\no\tc\tb\tblocks\t2026-08-02 10:00:00.000\n", 4)
	if len(want) != 2 || want["o\ta\tb\tblocks"] != "2026-08-01 10:00:00.000" {
		t.Fatalf("keyed values = %v", want)
	}
	if problems := viewFilledRowProblems("work_item_dependency_first_seen", rule, want, want); len(problems) != 0 {
		t.Fatalf("the table holds what the rule gives: %v", problems)
	}
	// PLANT 2: a view that writes a wrong first seen.
	wrong := map[string]string{"o\ta\tb\tblocks": "2026-07-31 10:00:00.000", "o\tc\tb\tblocks": "2026-08-02 10:00:00.000"}
	if problems := viewFilledRowProblems("work_item_dependency_first_seen", rule, want, wrong); len(problems) != 1 ||
		!strings.Contains(problems[0], "first_seen_at of o\ta\tb\tblocks is 2026-07-31 10:00:00.000, want min(last_synced) = 2026-08-01 10:00:00.000") {
		t.Fatalf("a wrong first seen: %q", problems)
	}
	// PLANT 3: a view that writes no row.
	if problems := viewFilledRowProblems("work_item_dependency_first_seen", rule, want, map[string]string{}); len(problems) != 2 ||
		!strings.Contains(problems[0], "no row for o\ta\tb\tblocks") || !strings.Contains(problems[1], "no row for o\tc\tb\tblocks") {
		t.Fatalf("a view that wrote no row: %q", problems)
	}
	// A row for a relation the source does not hold.
	extra := map[string]string{"o\ta\tb\tblocks": "2026-08-01 10:00:00.000", "o\tc\tb\tblocks": "2026-08-02 10:00:00.000", "o\tx\ty\tblocks": "2026-08-03 10:00:00.000"}
	if problems := viewFilledRowProblems("work_item_dependency_first_seen", rule, want, extra); len(problems) != 1 || !strings.Contains(problems[0], "a row for o\tx\ty\tblocks, which work_item_dependencies does not hold") {
		t.Fatalf("a row from nowhere: %q", problems)
	}

	// ---- a table the load does not write ----
	before := map[string]string{"repos": "0", "work_item_dependencies": "0", "work_item_dependency_first_seen": "0", "git_blame": "4"}
	after := map[string]string{"repos": "0", "work_item_dependencies": "7", "work_item_dependency_first_seen": "7", "git_blame": "4"}
	wrote := map[string]bool{"work_item_dependencies": true}
	if problems := unwrittenTableProblems(before, after, wrote); len(problems) != 0 {
		t.Fatalf("the view-filled table changed with the table its view reads: %v", problems)
	}
	// PLANT 4: a table NOT in the list that the load changes.
	after["git_blame"] = "5"
	if problems := unwrittenTableProblems(before, after, wrote); len(problems) != 1 || problems[0] != "the load changed git_blame (4 -> 5), which it does not write" {
		t.Fatalf("a table that is not in the list changed: %q", problems)
	}
	after["git_blame"] = "4"
	// The view-filled table changed in a load that does NOT write the table
	// its view reads: nothing should have filled it.
	if problems := unwrittenTableProblems(before, after, map[string]bool{"repos": true}); len(problems) != 2 ||
		problems[0] != "the load changed work_item_dependencies (0 -> 7), which it does not write" ||
		problems[1] != "the load changed work_item_dependency_first_seen (0 -> 7), which it does not write" {
		t.Fatalf("a view-filled table changed with no write to its source: %q", problems)
	}
}

// The same rules on a live ClickHouse at the migration head: the real column,
// the real view. Each plant is one defect; each is reported by name.
func TestAfterCaptureRulesCatchPlantedDefectsOnALiveClickHouse(t *testing.T) {
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	stopMerges(t, ch.httpDSN)
	const org = "11111111-2222-4333-8444-555555555555"
	relation := func(source, target, lastSynced string, extraColumns, extraValues string) {
		t.Helper()
		clickHouseHTTP(t, ch.httpDSN, "INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, relationship_semantics_version, last_synced, org_id"+extraColumns+
			") VALUES ('"+source+"', '"+target+"', 'blocks', 'blocks', 'canonical-blocks.v2', '"+lastSynced+"', '"+org+"'"+extraValues+")")
	}
	columnProblems := func() []string {
		t.Helper()
		_, problems := withoutColumnsAfterTheCapture(dumpWorldTable(t, ch.httpDSN, "work_item_dependencies"))
		return problems
	}

	// A load as the loader does it: the relation rows, the new column not named.
	// The same relation twice (two syncs): the first sync is the first seen.
	before := rowCounts(t, ch.httpDSN)
	relation("jira:A-1", "jira:A-2", "2026-08-05 10:00:00.000", "", "")
	relation("jira:A-1", "jira:A-2", "2026-08-01 10:00:00.000", "", "")
	relation("jira:A-3", "jira:A-2", "2026-08-02 10:00:00.000", "", "")
	if problems := columnProblems(); len(problems) != 0 {
		t.Fatalf("a load that does not name the new column: %v", problems)
	}
	if problems := viewFilledProblems(t, ch.httpDSN); len(problems) != 0 {
		t.Fatalf("the real view after a clean load: %v", problems)
	}
	if got := strings.TrimSpace(clickHouseHTTP(t, ch.httpDSN, "SELECT toString(min(first_seen_at)) FROM work_item_dependency_first_seen WHERE source_work_item_id = 'jira:A-1' FORMAT TSV")); got != "2026-08-01 10:00:00.000" {
		t.Fatalf("first seen of the relation synced twice = %q, want the first sync", got)
	}
	wrote := map[string]bool{"work_item_dependencies": true}
	if problems := unwrittenTableProblems(before, rowCounts(t, ch.httpDSN), wrote); len(problems) != 0 {
		t.Fatalf("a clean load: %v", problems)
	}

	// PLANT 4: the load also changes a table that is in no list.
	clickHouseHTTP(t, ch.httpDSN, "INSERT INTO work_item_reopen_events (org_id, work_item_id, occurred_at, from_status, to_status, last_synced) VALUES ('"+org+"', 'jira:A-2', now64(3), 'done', 'todo', now64(3))")
	if problems := unwrittenTableProblems(before, rowCounts(t, ch.httpDSN), wrote); len(problems) != 1 || !strings.HasPrefix(problems[0], "the load changed work_item_reopen_events (") {
		t.Fatalf("a table in no list changed: %q", problems)
	}

	// PLANT 1: a load that writes a value into the new column.
	relation("jira:A-4", "jira:A-2", "2026-08-03 10:00:00.000", ", relation_started_at", ", '2026-07-01 08:00:00.000'")
	if problems := columnProblems(); len(problems) != 1 ||
		!strings.HasPrefix(problems[0], "work_item_dependencies.relation_started_at (added by 102_work_item_dependency_first_seen.sql, after the capture): 1 of 4 loaded rows hold a value") {
		t.Fatalf("a load that wrote the new column: %q", problems)
	}
	if problems := viewFilledProblems(t, ch.httpDSN); len(problems) != 0 {
		t.Fatalf("the view is still right after plant 1: %v", problems)
	}

	// work_item_relations_read (migration 103): a text row and a later board
	// row of one item, and a board row alone of another. The view keeps the
	// text row only; the rule, with the view's filter, agrees. The same rule
	// WITHOUT the filter (an in-test plant of the check) reports both items.
	item := func(id, project, lastSynced string) {
		t.Helper()
		clickHouseHTTP(t, ch.httpDSN, "INSERT INTO work_items (repo_id, work_item_id, provider, project_id, last_synced, org_id) VALUES (toUUID('00000000-0000-0000-0000-000000000000'), '"+id+"', 'github', '"+project+"', '"+lastSynced+"', '"+org+"')")
	}
	item("gh:acme/api#7", "acme/api", "2026-08-05 10:00:00.000")
	item("gh:acme/api#7", "ghprojv2:acme#1", "2026-08-07 10:00:00.000")
	item("gh:acme/api#8", "ghprojv2:acme#1", "2026-08-07 10:00:00.000")
	readRule := viewFilledAfterTheCapture["work_item_relations_read"]
	if problems := viewFilledTableProblems(t, ch.httpDSN, "work_item_relations_read", readRule); len(problems) != 0 {
		t.Fatalf("the real view of work_item_relations_read: %v", problems)
	}
	if got := strings.TrimSpace(clickHouseHTTP(t, ch.httpDSN, "SELECT toString(max(relations_read_at)) FROM work_item_relations_read WHERE work_item_id = 'gh:acme/api#7' FORMAT TSV")); got != "2026-08-05 10:00:00.000" {
		t.Fatalf("read time of #7 = %q, want the text row's, not the board row's", got)
	}
	unfiltered := readRule
	unfiltered.SourceWhere = ""
	if problems := viewFilledTableProblems(t, ch.httpDSN, "work_item_relations_read", unfiltered); len(problems) != 2 ||
		!strings.Contains(problems[0], "relations_read_at of "+org+"\tgh:acme/api#7 is 2026-08-05 10:00:00.000, want max(last_synced) = 2026-08-07 10:00:00.000") ||
		!strings.Contains(problems[1], "no row for "+org+"\tgh:acme/api#8") {
		t.Fatalf("the rule without the view's filter: %q", problems)
	}

	// PLANT 2: a view that writes a wrong first seen (one day early).
	clickHouseHTTP(t, ch.httpDSN, "DROP VIEW work_item_dependency_first_seen_mv")
	clickHouseHTTP(t, ch.httpDSN, "CREATE MATERIALIZED VIEW work_item_dependency_first_seen_mv TO work_item_dependency_first_seen AS SELECT org_id, source_work_item_id, target_work_item_id, relationship_type, min(last_synced) - INTERVAL 1 DAY AS first_seen_at FROM work_item_dependencies GROUP BY org_id, source_work_item_id, target_work_item_id, relationship_type")
	relation("jira:A-5", "jira:A-2", "2026-08-04 10:00:00.000", "", "")
	problems := viewFilledProblems(t, ch.httpDSN)
	if len(problems) != 1 || !strings.Contains(problems[0], "first_seen_at of "+org+"\tjira:A-5\tjira:A-2\tblocks is 2026-08-03 10:00:00.000, want min(last_synced) = 2026-08-04 10:00:00.000") {
		t.Fatalf("a view that writes a wrong first seen: %q", problems)
	}

	// PLANT 3: no view, so no row for a new relation.
	clickHouseHTTP(t, ch.httpDSN, "DROP VIEW work_item_dependency_first_seen_mv")
	relation("jira:A-6", "jira:A-2", "2026-08-06 10:00:00.000", "", "")
	problems = viewFilledProblems(t, ch.httpDSN)
	if len(problems) != 3 || !strings.Contains(problems[0], "the materialized view work_item_dependency_first_seen_mv does not exist") ||
		!strings.Contains(problems[1], "first_seen_at of "+org+"\tjira:A-5\tjira:A-2\tblocks") ||
		!strings.Contains(problems[2], "no row for "+org+"\tjira:A-6\tjira:A-2\tblocks") {
		t.Fatalf("a view that writes no row: %q", problems)
	}
}

// The view of work_item_relations_read keeps every work_items row except a
// github Projects v2 board row (migration 103, CHAOS-8578); the rule's read of
// the source must keep the same rows, or the check compares the table with
// rows the view never saw. The first-seen view has no filter.
func TestViewFilledRulesReadTheSourceRowsTheirViewKeeps(t *testing.T) {
	read := viewFilledAfterTheCapture["work_item_relations_read"]
	want, got := viewFilledQueries("work_item_relations_read", read)
	if wantText := "FROM `work_items` WHERE NOT (provider = 'github' AND startsWith(project_id, 'ghprojv2:')) GROUP BY `org_id`, `work_item_id`"; !strings.Contains(want, wantText) {
		t.Fatalf("the rule's read of work_items = %q, want it to hold %q", want, wantText)
	}
	if !strings.Contains(got, "toString(max(`relations_read_at`)) FROM `work_item_relations_read`") || strings.Contains(got, "WHERE") {
		t.Fatalf("the table read = %q", got)
	}
	firstSeen, _ := viewFilledQueries("work_item_dependency_first_seen", viewFilledAfterTheCapture["work_item_dependency_first_seen"])
	if strings.Contains(firstSeen, "WHERE") {
		t.Fatalf("the first-seen rule has no filter, but its read is %q", firstSeen)
	}
}
