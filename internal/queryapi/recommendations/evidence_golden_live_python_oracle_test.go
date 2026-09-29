package recommendations

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// testdata/evidence_golden.json is what Python's real row mapper answers for
// the evidence columns of the grid. This test EXECUTES the generator
// (testdata/gen_evidence_golden.py) against the current production Python and
// requires its output to equal the checked-in file byte for byte, so the
// golden cannot go stale when the Python mapper changes: the run fails and
// says how to regenerate.
func TestEvidenceGoldenIsWhatPythonProducesNow(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

	command := exec.Command(python, filepath.Join("testdata", "gen_evidence_golden.py"))
	command.Dir = filepath.Dir(file)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	golden, err := os.ReadFile("testdata/evidence_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), golden) {
		t.Fatalf("testdata/evidence_golden.json is stale: Python's row mapper now answers differently. Regenerate it with the command in testdata/gen_evidence_golden.py and review the diff.")
	}
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" && !t.Failed() {
		if err := os.WriteFile(filepath.Join(proof, "query-api-recommendations-evidence"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
