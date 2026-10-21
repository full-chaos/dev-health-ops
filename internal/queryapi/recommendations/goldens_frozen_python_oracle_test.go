package recommendations

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// testdata/evidence_golden.json is what Python's real row mapper answers for
// the evidence columns of the grid, and testdata/window_golden.json what its
// real _window_to_dates answers for the (unit, value) grid, and
// testdata/failure_modes_golden.json what its real resolve_recommendations
// answers when the window overflows or the ClickHouse read fails. This test compares
// each generator's frozen answer (the generator executed once on the pinned
// build) with the checked-in file byte for byte, so a golden cannot be edited
// by hand.
func TestGoldensAreWhatPythonProducesNow(t *testing.T) {
	pairs := [][2]string{
		{"gen_evidence_golden.py", "evidence_golden.json"},
		{"gen_window_golden.py", "window_golden.json"},
		{"gen_failure_modes_golden.py", "failure_modes_golden.json"},
	}
	programs := make([]programoracle.Program, len(pairs))
	for index, pair := range pairs {
		programs[index] = programoracle.Program{Name: pair[0]}
	}
	outputs := frozenPython(t, "generators.golden.json", programs...)
	for index, pair := range pairs {
		golden, err := os.ReadFile(filepath.Join("testdata", pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if outputs[index] != string(golden) {
			t.Errorf("testdata/%s is not what the frozen answer of %s says", pair[1], pair[0])
		}
	}
}
