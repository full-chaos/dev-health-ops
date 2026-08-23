package cpyrandom

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	cpyRandomProofFile       = "cpython-random-golden"
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
func TestGoldenStillDescribesLiveCPython(t *testing.T) {
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv(livePythonOracleProofDir)
	if proofDirectory == "" {
		t.Fatalf("%s is required", livePythonOracleProofDir)
	}

	root := repoRoot(t)
	generator := filepath.Join(root, "tests", "fixtures", "generate_cpython_random_golden.py")
	golden := filepath.Join(root, "tests", "fixtures", "cpython_random_golden.json")

	python := os.Getenv("PYTHON")
	if python == "" {
		resolved, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the cpython-random rot guard: %v", err)
		}
		python = resolved
	}

	// --check re-derives every vector from the LIVE interpreter and compares.
	// The generator owns that comparison so there is only one definition of
	// what the vectors are, rather than a second copy here that could drift
	// from the producer in its own way.
	command := exec.Command(python, generator, "--check", golden)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf(
			"the recorded CPython vectors no longer match this interpreter.\n"+
				"Either CPython changed its random stream (in which case the "+
				"capacity port's parity claim needs re-examining, not just a "+
				"regenerated file) or the generator was edited.\n%s",
			output,
		)
	}
	if !strings.Contains(string(output), "CPYTHON_RANDOM_GOLDEN_CURRENT") {
		t.Fatalf(
			"the check produced no positive marker, so a silent no-op cannot be "+
				"distinguished from a pass:\n%s", output)
	}

	// Only on a PASS. t.Fatalf above already halts, but an added t.Errorf
	// later would not, and a marker that can mean either outcome is not
	// evidence.
	if t.Failed() {
		return
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, cpyRandomProofFile), []byte("executed\n"), 0o600,
	); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}
