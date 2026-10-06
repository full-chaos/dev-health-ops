package providersync

import "fmt"

// workItemInteractionColumnsAfterThePythonFreeze names every column of
// work_item_interactions that a migration added after the Python producers and
// sinks were frozen (CHAOS-8790). Go writes it; no frozen Python output can
// hold it. A frozen-oracle comparison of an interaction insert leaves out
// exactly this and nothing else: this is the ONE list.
var workItemInteractionColumnsAfterThePythonFreeze = map[string]string{
	"interaction_id": "migration 107 (CHAOS-8790): the provider's own comment id, last in the sorting key, written by every Go normalizer; the Python producers and sink were frozen before the column existed",
}

// withoutInteractionColumnsAfterThePythonFreeze returns the Go insert's columns
// and rows of work_item_interactions without the columns of
// workItemInteractionColumnsAfterThePythonFreeze. A named column the insert
// does not write is a problem (the entry has rotted), and so is a row that
// holds a value in one: a frozen case carries no such fact, so a value there
// is something the comparison would silently drop.
func withoutInteractionColumnsAfterThePythonFreeze(columns []string, rows [][]any) ([]string, [][]any, []string) {
	var problems []string
	drop := map[int]bool{}
	for name := range workItemInteractionColumnsAfterThePythonFreeze {
		position := -1
		for index, column := range columns {
			if column == name {
				position = index
			}
		}
		if position < 0 {
			problems = append(problems, fmt.Sprintf("work_item_interactions column %q is named after the Python freeze but the Go insert does not write it: the entry has rotted", name))
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
			if text, isString := value.(string); !isString || text != "" {
				problems = append(problems, fmt.Sprintf("row %d writes %v into %q, a column no frozen Python case can hold: a frozen case must leave it empty", rowIndex, value, columns[index]))
			}
		}
		keptRows = append(keptRows, kept)
	}
	return keptColumns, keptRows, problems
}

// workItemInteractionIDGoOnly is the goOnlyFields entry of every row-level
// oracle of an interaction: the frozen Python semantic row has no comment id
// (CHAOS-8790), Go carries the provider's.
var workItemInteractionIDGoOnly = map[string]string{
	"interaction_id": "Python's WorkItemInteractionEvent has no comment id: the " +
		"sorting key of work_item_interactions gained it after the Python freeze " +
		"(migration 107, CHAOS-8790). Go carries the provider's own comment id so " +
		"two comments of one work item in the same millisecond stay two rows.",
}
