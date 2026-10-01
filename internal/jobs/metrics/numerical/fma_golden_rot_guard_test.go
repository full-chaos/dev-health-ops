package numerical

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestFMAGoldenMatchesLivePython is the rot guard for tests/fixtures/fma_golden.json,
// generated from production Python (release_impact._compute_confidence,
// compute._percentile, compute_capacity._percentile) and frozen. The generator's
// stdout was executed once on the last build that carried the Python sources and
// is frozen in testdata/golden/fma_rot_guard.json; a frozen run compares that
// recorded stdout byte for byte with the checked-in fixture, so the guard fails
// when the fixture is edited without recording the producer again. No Python
// runs in a frozen run.
func TestFMAGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRoot(t)
	spec := rotguard.Spec("testdata/golden/fma_rot_guard.json", "deb571e7b2d94fb8b3f7ffc6e1309daa136ae001942e703954b0c20576a5a5ba",
		"./internal/jobs/metrics/numerical/", "^TestFMAGoldenMatchesLivePython$")
	rendered := rotguard.Run(t, spec, root, rotguard.Generator{
		Name: "fma golden generator",
		Path: "tests/fixtures/generate_fma_golden.py",
		Args: []string{"--stdout"},
	})[0]

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "fma_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace([]byte(rendered))) {
		t.Errorf(
			"the recorded Python render no longer equals the checked-in FMA golden.\n" +
				"Either tests/fixtures/fma_golden.json was edited by hand or the generator changed. " +
				"Regenerate with\n    python tests/fixtures/generate_fma_golden.py\n" +
				"and review the diff as a real behaviour change -- if Go should follow, change Go " +
				"too and re-verify every fma_golden_test.go bit-exact test; if it should not, the " +
				"Python change is the bug -- and record the producer again (the golden's recipe).",
		)
	}
}
