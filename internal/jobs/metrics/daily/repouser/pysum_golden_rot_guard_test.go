package repouser

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestPysumGoldenMatchesLivePython is the CHAOS-4824 rot guard for
// tests/fixtures/pysum_golden.json: a frozen fixture with no regeneration guard
// measures history, not parity, and degrades silently the moment the Python
// reference changes.
//
// The generator's stdout (compute_code_ownership_gini / compute_pipeline_stability
// rendered from real Python) was executed once on the last build that carried the
// Python sources and is frozen in testdata/golden/pysum_rot_guard.json; a frozen
// run compares the recorded stdout with the checked-in fixture (see rotguard), so
// the guard still fails when the fixture is edited without recording the producer
// again, and no Python runs in a frozen run.
func TestPysumGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRootPath(t)
	spec := rotguard.Spec("testdata/golden/pysum_rot_guard.json", "ed18bc20ef2267236eb04e763322fd03429d4f3bbc42434930057220fc42adea")
	rendered := rotguard.Run(t, spec, rotguard.Generator{
		Name: "pysum golden generator",
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "pysum_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace([]byte(rendered))) {
		t.Errorf(
			"the recorded Python render no longer equals the checked-in pysum golden.\n" +
				"Either tests/fixtures/pysum_golden.json was edited by hand or the generator changed. " +
				"Review the diff as a real behaviour change -- if Go should follow, change Go " +
				"too and re-verify TestCodeOwnershipGiniMatchesLivePythonBitExact; if it should not, " +
				"the Python change is the bug -- and record the producer again.",
		)
	}
}
