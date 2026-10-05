package providersync

import (
	"fmt"
	"reflect"
	"testing"
)

// workItemDependencyColumnsAfterThePythonFreeze names every column of
// work_item_dependencies that a migration added after the Python producers and
// sinks were frozen. Go writes them; no frozen Python output can hold them. A
// frozen-oracle comparison of a dependency row or of its insert leaves out
// exactly these and nothing else: this is the ONE list.
var workItemDependencyColumnsAfterThePythonFreeze = map[string]string{
	"relation_started_at": "migration 102 (CHAOS-8574): the provider's own time of the link, written by the Go normalizers that read one (gitlab, linear; CHAOS-8578); the Python producers were frozen before the column existed",
	"relation_writer":     "migration 103 (CHAOS-8578): which items of the row write it, written by the Go gitlab normalizer; the Python producers were frozen before the column existed",
}

// withoutDependencyColumnsAfterThePythonFreeze returns the Go insert's columns
// and rows of work_item_dependencies without the columns of
// workItemDependencyColumnsAfterThePythonFreeze, so the rest can be compared
// with the frozen Python sink column for column. A named column that the
// insert does not write is a problem (the entry has rotted), and so is a row
// that holds a value in one: the frozen cases carry no such fact, so a value
// there is something the comparison would silently drop.
func withoutDependencyColumnsAfterThePythonFreeze(columns []string, rows [][]any) ([]string, [][]any, []string) {
	var problems []string
	drop := map[int]bool{}
	for name := range workItemDependencyColumnsAfterThePythonFreeze {
		position := -1
		for index, column := range columns {
			if column == name {
				position = index
			}
		}
		if position < 0 {
			problems = append(problems, fmt.Sprintf("work_item_dependencies column %q is named after the Python freeze but the Go insert does not write it: the entry has rotted", name))
			continue
		}
		drop[position] = true
	}
	keptColumns := make([]string, 0, len(columns))
	for index, column := range columns {
		if !drop[index] {
			keptColumns = append(keptColumns, column)
		}
	}
	keptRows := make([][]any, 0, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(columns) {
			problems = append(problems, fmt.Sprintf("row %d has %d values for %d columns", rowIndex, len(row), len(columns)))
			continue
		}
		kept := make([]any, 0, len(keptColumns))
		for index, value := range row {
			if !drop[index] {
				kept = append(kept, value)
				continue
			}
			if !isNilValue(value) {
				problems = append(problems, fmt.Sprintf("row %d writes %v into %q, a column no frozen Python case can hold: a frozen case must leave it empty", rowIndex, value, columns[index]))
			}
		}
		keptRows = append(keptRows, kept)
	}
	return keptColumns, keptRows, problems
}

// isNilValue reports whether value is nil, or a typed nil pointer.
func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}

// The list's own rules: a named column the insert does not write, and a row
// with a value in a named column, are each a problem; the rest is kept in
// order.
func TestDependencyColumnsAfterThePythonFreezeAreLeftOutOnlyWhenEmpty(t *testing.T) {
	columns := insertColumns(t, gitHubWorkItemDependenciesInsert)
	written := githubWorkItemDependencyRow{SourceWorkItemID: "a", TargetWorkItemID: "b", RelationshipType: "blocks"}
	kept, rows, problems := withoutDependencyColumnsAfterThePythonFreeze(columns, [][]any{workItemDependencyValues(written)})
	if len(problems) != 0 {
		t.Fatalf("problems for an insert with every named column empty: %v", problems)
	}
	if len(kept) != len(columns)-len(workItemDependencyColumnsAfterThePythonFreeze) || len(rows[0]) != len(kept) {
		t.Fatalf("kept %d columns and %d values of %d columns", len(kept), len(rows[0]), len(columns))
	}
	for _, column := range kept {
		if _, named := workItemDependencyColumnsAfterThePythonFreeze[column]; named {
			t.Fatalf("the named column %q is kept", column)
		}
	}

	writer := "both"
	written.RelationWriter = &writer
	if _, _, problems := withoutDependencyColumnsAfterThePythonFreeze(columns, [][]any{workItemDependencyValues(written)}); len(problems) != 1 {
		t.Fatalf("a row with a value in a named column: problems %v, want one", problems)
	}
	written.RelationWriter = nil
	short, shortRow := columns[:len(columns)-1], workItemDependencyValues(written)[:len(columns)-1]
	if _, _, problems := withoutDependencyColumnsAfterThePythonFreeze(short, [][]any{shortRow}); len(problems) != 1 {
		t.Fatalf("an insert that does not write a named column: problems %v, want one", problems)
	}
}
