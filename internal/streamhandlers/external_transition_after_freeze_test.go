package streamhandlers

import "fmt"

// withoutTransitionProviderAfterThePythonFreeze checks, then drops, the
// provider column of a work_item_transition.v1 row for the comparison with the
// frozen Python golden, which wrote no provider: the value must be the batch
// source system, and a missing column or another value is a problem.
func withoutTransitionProviderAfterThePythonFreeze(columns []string, values []any, system string) ([]string, []any, string) {
	position := -1
	for index, column := range columns {
		if column == "provider" {
			position = index
		}
	}
	if position < 0 || len(values) != len(columns) {
		return columns, values, fmt.Sprintf("work_item_transitions writes no provider value: %d columns, %d values", len(columns), len(values))
	}
	if text, isString := values[position].(string); !isString || text != system {
		return columns, values, fmt.Sprintf("work_item_transitions provider = %v, want the source system %q", values[position], system)
	}
	keptColumns := append(append([]string{}, columns[:position]...), columns[position+1:]...)
	keptValues := append(append([]any{}, values[:position]...), values[position+1:]...)
	return keptColumns, keptValues, ""
}
