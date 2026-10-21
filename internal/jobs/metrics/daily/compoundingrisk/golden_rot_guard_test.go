package compoundingrisk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestCompoundingRiskGoldenMatchesLivePython is the rot guard for
// tests/fixtures/daily_compounding_risk_python_golden.json (CHAOS-4287): the
// frozen file was generated from REAL Python (compute_compounding_risk) and
// checked in; TestComputeMatchesFrozenPythonGolden asserts Go matches it, and
// this asserts the Python that rendered it still renders the same bytes.
//
// The generator's stdout was executed once on the last build that carried the
// Python sources and is frozen in testdata/golden/compounding_risk_rot_guard.json
// ; a frozen run compares that recorded stdout with
// the checked-in fixture, so the guard fails the moment the fixture is edited
// without recording the producer again, and a changed generator is another
// request. No Python runs in a frozen run.
func TestCompoundingRiskGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRoot(t)
	spec := rotguard.Spec("testdata/golden/compounding_risk_rot_guard.json", "7b1603598242c659433434668462c80b33e7ca3db75ec427a32d12f22e49afeb")
	rendered := rotguard.Run(t, spec, rotguard.Generator{
		Name: "compounding risk golden generator",
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "daily_compounding_risk_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frozen, []byte(rendered)) {
		t.Errorf(
			"the recorded Python render no longer equals the checked-in golden.\n" +
				"Either tests/fixtures/daily_compounding_risk_python_golden.json was edited by hand or the generator changed. " +
				"Review the diff as a real behaviour change -- if Go should follow, port the change " +
				"into compute.go and update compute_test.go's goldenCases too -- and record the producer again.",
		)
	}
}
