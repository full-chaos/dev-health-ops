package externalingest

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// oracleRoot, runPython and writeOracleProof are the live-Python oracle
// helpers this package's tests share; their siblings in recordvalidation are
// the same three functions (a _test.go symbol cannot be imported across
// packages).
func oracleRoot(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return root, pyoracle.Resolve(t, root)
}

func runPython(t *testing.T, root, python, program string, stdin []byte) []byte {
	t.Helper()
	command := exec.Command(python, "-c", program)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(stdin)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	return output
}

func writeOracleProof(t *testing.T, name string) {
	t.Helper()
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, name), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
