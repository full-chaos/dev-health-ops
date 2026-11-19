package providersync

import "fmt"

// workItemTransitionColumnsAfterThePythonFreeze names every column of work_item_transitions that a migration added
// after the Python producers and sinks were frozen. Go writes it; no frozen Python output can hold it. A frozen-oracle
// comparison of a transition insert leaves out exactly this and nothing else: this is the ONE list.
var workItemTransitionColumnsAfterThePythonFreeze = map[string]string{
	"tie_ordinal": "migration 170 (CHAOS-14190): the rank of a transition among the item's transitions at one instant, last in the sorting key, 0 for the only one; the Python sink was frozen before the column existed",
}

// withoutTransitionColumnsAfterThePythonFreeze returns the Go insert's columns and rows of work_item_transitions
// without the columns of workItemTransitionColumnsAfterThePythonFreeze. A named column the insert does not write is a
// problem (the entry has rotted), and so is a row that holds a value other than 0 there: a frozen case is one
// transition of its instant.
func withoutTransitionColumnsAfterThePythonFreeze(columns []string, rows [][]any) ([]string, [][]any, []string) {
	var problems []string
	drop := map[int]bool{}
	for name := range workItemTransitionColumnsAfterThePythonFreeze {
		position := -1
		for index, column := range columns {
			if column == name {
				position = index
			}
		}
		if position < 0 {
			problems = append(problems, fmt.Sprintf("work_item_transitions column %q is named after the Python freeze but the Go insert does not write it: the entry has rotted", name))
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
			if ordinal, isOrdinal := value.(uint16); !isOrdinal || ordinal != 0 {
				problems = append(problems, fmt.Sprintf("row %d writes %v into %q: a frozen case is the only transition of its instant, so it must be 0", rowIndex, value, columns[index]))
			}
		}
		keptRows = append(keptRows, kept)
	}
	return keptColumns, keptRows, problems
}
