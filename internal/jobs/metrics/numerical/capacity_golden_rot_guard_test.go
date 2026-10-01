package numerical

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestCapacityForecastGoldenMatchesLivePython is the rot guard for
// tests/fixtures/capacity_forecast_golden.json. Two claims are needed and they
// are different:
//
//	Go      == recording   (TestCapacityForecastMatchesPythonGolden)
//	CPython == recording   (this test)
//
// Only both together mean "Go reproduces Python". With only the first, a change
// to the Python forecast semantics leaves the frozen file encoding the OLD
// behaviour, Go keeps matching the file, and the parity claim stays green while
// the two implementations have diverged.
//
// The generator renders every case from the real producer; its own --check mode
// compared the "cases" and "date_cases" sections with the checked-in file. The
// generator's stdout was executed once on the last build that carried the Python
// sources and is frozen in testdata/golden/capacity_forecast_rot_guard.json; a
// frozen run makes the same comparison on the recorded stdout (no Python runs).
func TestCapacityForecastGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRoot(t)
	spec := rotguard.Spec("testdata/golden/capacity_forecast_rot_guard.json", "PIN:capacity_forecast_rot_guard",
		"./internal/jobs/metrics/numerical/", "^TestCapacityForecastGoldenMatchesLivePython$")
	rendered := rotguard.Run(t, spec, root, rotguard.Generator{
		Name: "capacity forecast golden generator",
		Path: "tests/fixtures/generate_capacity_forecast_golden.py",
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "capacity_forecast_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	live, checkedIn := decodeExact(t, []byte(rendered), "recorded generator output"), decodeExact(t, frozen, "checked-in golden")
	for _, key := range []string{"cases", "date_cases"} {
		if _, present := live[key]; !present {
			t.Fatalf("the recorded generator output has no %q section: the comparison would pass on nothing", key)
		}
		if !reflect.DeepEqual(checkedIn[key], live[key]) {
			t.Errorf(
				"the recorded capacity forecasts no longer match the checked-in golden in %q.\n"+
					"This is producer drift, not necessarily a Go bug: the frozen file was generated "+
					"from production Python, and the parity test only proves Go matches the FILE. "+
					"Regenerate with\n    python tests/fixtures/generate_capacity_forecast_golden.py\n"+
					"read the diff as a real behaviour change -- if Go should follow, Go changes too -- "+
					"and record the producer again (the golden's recipe).", key)
		}
	}
}

// decodeExact decodes JSON without collapsing numbers into float64: a numeric
// golden compared through float64 would let a genuine precision drift round
// away to equal.
func decodeExact(t *testing.T, raw []byte, label string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}
	return value
}

// repositoryRoot walks up from this package to the checkout root.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	return rotguard.RepositoryRoot(t, 4)
}
