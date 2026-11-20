package providersync

import "fmt"

// withoutTransitionProviderAfterThePythonFreeze checks, then drops, the
// provider column of a work_item_transitions insert for a frozen-oracle
// comparison: the frozen Python sink left provider at its default, Go writes
// the row's provider. Every row must hold want there; a missing column, a
// value of another type or another provider is a problem, never dropped.
func withoutTransitionProviderAfterThePythonFreeze(columns []string, rows [][]any, want string) ([]string, [][]any, []string) {
	position := -1
	for index, column := range columns {
		if column == "provider" {
			position = index
		}
	}
	if position < 0 {
		return columns, rows, []string{"work_item_transitions insert does not write provider"}
	}
	var problems []string
	keptColumns := append(append([]string{}, columns[:position]...), columns[position+1:]...)
	keptRows := make([][]any, 0, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(columns) {
			problems = append(problems, fmt.Sprintf("row %d has %d values for %d columns", rowIndex, len(row), len(columns)))
			continue
		}
		if text, isString := row[position].(string); !isString || text != want {
			problems = append(problems, fmt.Sprintf("row %d writes provider %v, want %q", rowIndex, row[position], want))
		}
		keptRows = append(keptRows, append(append([]any{}, row[:position]...), row[position+1:]...))
	}
	return keptColumns, keptRows, problems
}
