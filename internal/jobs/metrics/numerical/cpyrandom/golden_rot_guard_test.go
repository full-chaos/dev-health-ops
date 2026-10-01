package cpyrandom

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestGoldenStillDescribesLiveCPython is the rot guard for the frozen vectors.
//
// TestGoStreamMatchesCPython proves the Go port matches the RECORDING. It
// cannot notice the recording drifting away from its producer, and that is a
// real failure mode with precedent in this repo: the numerical golden
// (remaining_metrics_python_golden.json) was frozen in July with its
// generator's --check mode wired to nothing, so for weeks it asserted Go
// matched a file while nothing asserted the file still matched Python.
//
// Two claims are needed and they are different:
//
//	Go   == recording   (TestGoStreamMatchesCPython, no interpreter, always runs)
//	CPython == recording (this test, live interpreter, lane-gated)
//
// Only both together mean "Go reproduces CPython". If CPython ever changes its
// stream -- a seeding change, a _randbelow change -- this fails loudly rather
// than letting the capacity port keep matching a stale artefact.
//
// The generator's stdout was executed once on the last build's interpreter and is
// frozen in testdata/golden/cpython_random_rot_guard.json; a frozen run makes the same
// comparison its --check mode made (the "cases" section) on the recorded stdout, with no
// interpreter. A CPython that moves is therefore found by recording again, not by this
// test failing on its own: the recording is the final record of the stream.

func TestGoldenStillDescribesLiveCPython(t *testing.T) {
	root := repoRoot(t)
	spec := rotguard.Spec("testdata/golden/cpython_random_rot_guard.json", "",
		"./internal/jobs/metrics/numerical/cpyrandom/", "^TestGoldenStillDescribesLiveCPython$")
	rendered := rotguard.Run(t, spec, root, rotguard.Generator{
		Name: "cpython random golden generator",
		Path: "tests/fixtures/generate_cpython_random_golden.py",
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "cpython_random_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	live, checkedIn := decodeExact(t, []byte(rendered), "recorded generator output"), decodeExact(t, frozen, "checked-in golden")
	if _, present := live["cases"]; !present {
		t.Fatal("the recorded generator output has no \"cases\" section: the comparison would pass on nothing")
	}
	if !reflect.DeepEqual(checkedIn["cases"], live["cases"]) {
		t.Errorf(
			"the recorded CPython vectors no longer match the checked-in golden.\n" +
				"Either the golden was edited by hand, CPython changed its random stream (in which case the " +
				"capacity port's parity claim needs re-examining, not just a regenerated file) or the generator " +
				"was edited; record the producer again (the golden's recipe).",
		)
	}
}

// decodeExact decodes JSON without collapsing numbers into float64.
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
