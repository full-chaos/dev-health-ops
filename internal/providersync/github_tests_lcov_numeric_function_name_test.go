package providersync

import (
	"testing"
	"time"
)

// A function name that starts with a number ("12,handler") reads as FN:<line>,<name> or FN:<start>,<end>,<name>, so its FNDA
// matches in either reading. Each FNDA record is used once: one file with "handler" and "12,handler" stores covered 1 of 2.
func TestLCOVNumericPrefixFunctionNameSharesNoFNDA(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cells := []struct{ name, body string }{
		{"FNDA by full names", "SF:a.go\nFN:1,handler\nFN:2,12,handler\nFNDA:1,handler\nFNDA:0,12,handler\nend_of_record\n"},
		{"FNDA by full names, hits swapped", "SF:a.go\nFN:1,handler\nFN:2,12,handler\nFNDA:0,handler\nFNDA:1,12,handler\nend_of_record\n"},
		{"FNDA by the bare name twice", "SF:a.go\nFN:1,handler\nFN:2,12,handler\nFNDA:1,handler\nFNDA:0,handler\nend_of_record\n"},
		{"declarations and records in the other order", "SF:a.go\nFN:2,12,handler\nFN:1,handler\nFNDA:0,12,handler\nFNDA:1,handler\nend_of_record\n"},
	}
	for _, cell := range cells {
		row, err := parseLCOVRow([]byte(cell.body), "c.info", "a", "r", "1", "o", now)
		if err != nil {
			t.Errorf("CELL %s: %v", cell.name, err)
			continue
		}
		if row.FunctionsTotal == nil || *row.FunctionsTotal != 2 || row.FunctionsCovered == nil || *row.FunctionsCovered != 1 {
			t.Errorf("CELL %s: stored functions %v/%v, want 2/1", cell.name, row.FunctionsTotal, row.FunctionsCovered)
		}
	}
}
