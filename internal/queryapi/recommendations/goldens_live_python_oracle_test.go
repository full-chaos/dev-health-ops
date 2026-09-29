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
// the evidence columns of the grid, and testdata/window_golden.json what its
// real _window_to_dates answers for the (unit, value) grid, and
// testdata/failure_modes_golden.json what its real resolve_recommendations
// answers when the window overflows or the ClickHouse read fails. This test EXECUTES
// each generator against the current production Python and requires its output
// to equal the checked-in file byte for byte, so a golden cannot go stale when
// the Python side changes: the run fails and says how to regenerate.
func TestGoldensAreWhatPythonProducesNow(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

	for _, pair := range [][2]string{
		{"gen_evidence_golden.py", "evidence_golden.json"},
		{"gen_window_golden.py", "window_golden.json"},
		{"gen_failure_modes_golden.py", "failure_modes_golden.json"},
	} {
		command := exec.Command(python, filepath.Join("testdata", pair[0]))
		command.Dir = filepath.Dir(file)
		command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		if err := command.Run(); err != nil {
			t.Fatalf("live python (%s): %v", pair[0], pyoracle.RunError(python, err, stderr.Bytes()))
		}
		golden, err := os.ReadFile(filepath.Join("testdata", pair[1]))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(stdout.Bytes(), golden) {
			t.Errorf("testdata/%s is stale: Python now answers differently. Regenerate it with the command in testdata/%s and review the diff.", pair[1], pair[0])
		}
	}
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" && !t.Failed() {
		if err := os.WriteFile(filepath.Join(proof, "query-api-recommendations-evidence"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
