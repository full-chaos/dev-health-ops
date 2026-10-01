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
// (recipe in the golden's spec); a frozen run compares that recorded stdout with
// the checked-in fixture, so the guard fails the moment the fixture is edited
// without recording the producer again, and a changed generator is another
// request. No Python runs in a frozen run.
func TestCompoundingRiskGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRoot(t)
	spec := rotguard.Spec("testdata/golden/compounding_risk_rot_guard.json", "",
		"./internal/jobs/metrics/daily/compoundingrisk/", "^TestCompoundingRiskGoldenMatchesLivePython$")
	rendered := rotguard.Run(t, spec, root, rotguard.Generator{
		Name: "compounding risk golden generator",
		Path: "tests/fixtures/generate_daily_compounding_risk_python_golden.py",
		Args: []string{"--stdout"},
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "daily_compounding_risk_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frozen, []byte(rendered)) {
		t.Errorf(
			"the recorded Python render no longer equals the checked-in golden.\n" +
				"Either tests/fixtures/daily_compounding_risk_python_golden.json was edited by hand or the generator changed. " +
				"Regenerate the fixture on the pinned build with\n" +
				"    python tests/fixtures/generate_daily_compounding_risk_python_golden.py\n" +
				"review the diff as a real behaviour change -- if Go should follow, port the change " +
				"into compute.go and update compute_test.go's goldenCases too -- and record the producer again (the golden's recipe).",
		)
	}
}
