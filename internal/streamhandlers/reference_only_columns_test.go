package streamhandlers

import (
	"slices"
	"sort"
	"testing"
)

// externalReferenceOnlyColumns are columns the frozen Python reference writes
// and this writer does not, on purpose: the roster column `members` of the
// teams table is gone (CHAOS-9087), so a team.v1 record's `members` is
// accepted and not stored. The reference row is never edited; the comparison
// drops the column from it. An entry no compared row reaches fails the test
// (a stale divergence).
var externalReferenceOnlyColumns = map[string][]string{
	"teams": {"members"},
}

type referenceOnlyUse map[string]bool

// drop returns the reference columns and values without the declared
// reference-only columns of the table.
func (use referenceOnlyUse) drop(table string, columns []string, values []any) ([]string, []any) {
	declared := externalReferenceOnlyColumns[table]
	if len(declared) == 0 {
		return columns, values
	}
	keptColumns := make([]string, 0, len(columns))
	keptValues := make([]any, 0, len(values))
	for index, column := range columns {
		if slices.Contains(declared, column) {
			use[table+"."+column] = true
			continue
		}
		keptColumns = append(keptColumns, column)
		if index < len(values) {
			keptValues = append(keptValues, values[index])
		}
	}
	return keptColumns, keptValues
}

func (use referenceOnlyUse) checkReached(t *testing.T) {
	t.Helper()
	var stale []string
	for table, columns := range externalReferenceOnlyColumns {
		for _, column := range columns {
			if !use[table+"."+column] {
				stale = append(stale, table+"."+column)
			}
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("declared CHAOS-9087 reference-only columns that no reference row reached: %v", stale)
	}
}
