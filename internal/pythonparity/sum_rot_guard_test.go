package pythonparity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestSumGoldenMatchesLivePython is the rot guard for
// tests/fixtures/python_sum_python_golden.json.
//
// Its own marker, and the producer is the INTERPRETER ITSELF -- CPython's
// builtin sum(). Neumaier compensation arrived in 3.12 (gh-100425); before
// that, sum() WAS a naive accumulation. So this fixture's contents depend on
// the interpreter version with no diff anywhere in this repository, and the
// dependency runs in both directions: an upgrade could refine the algorithm
// again, and a DOWNGRADE below 3.12 would make naive summation correct and this
// package's compensation wrong.
//
// Two call sites depend on it: effort churn totals, whose result is compared
// against zero to select a metric tier, and the mean edge confidence, which
// feeds evidence_quality and its bands. In both, a last-bit difference can
// change a categorical output rather than a decimal.
func TestSumGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := parityRepositoryRoot(t)
	python := parityLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_python_sum_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("sum generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the sum generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(
		repoRoot, "tests", "fixtures", "python_sum_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "python-sum-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python's sum() no longer reproduces the frozen golden.\n" +
			"Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_python_sum_golden.py\n" +
			"then RE-READ rather than re-freezing. sum()'s float algorithm is a\n" +
			"property of the interpreter: Neumaier compensation since 3.12, naive\n" +
			"accumulation before it. If the interpreter moved, pythonparity.Sum must\n" +
			"move with it -- and a downgrade below 3.12 would make this package's\n" +
			"compensation WRONG rather than merely unnecessary.\n" +
			"The corpus is seeded, so a diff here is a behaviour change and never noise.",
	)
}
