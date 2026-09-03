package numerical

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	fmaLivePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	fmaLivePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	fmaGoldenProofFile          = "fma-golden"
)

// TestFMAGoldenMatchesLivePython is the CHAOS-4818 rot guard, in the shape
// established by TestRemainingMetricsGoldenMatchesLivePython
// (golden_rot_guard_test.go): tests/fixtures/fma_golden.json was generated
// from REAL production Python (release_impact._compute_confidence,
// compute._percentile, compute_capacity._percentile, hotspots.compute_file_hotspots)
// and then frozen. The bit-exact tests in fma_golden_test.go (and its
// per-package copies) only prove Go matches the FILE -- nothing proves
// Python still matches the file, so the moment any of those four functions'
// numerics change, the frozen `expected_bits` values silently encode the OLD
// Python behaviour and every bit-exact test stays green while the two
// implementations have actually diverged.
func TestFMAGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv(fmaLivePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv(fmaLivePythonOracleProofDir)
	if proofDirectory == "" {
		t.Fatalf("%s is required", fmaLivePythonOracleProofDir)
	}

	repoRoot := repositoryRoot(t)
	python := livePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_fma_golden.py")
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	rendered, err := exec.Command(python, generator, "--stdout").Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the FMA golden generator against live Python: %v: %s", err, stderr)
	}

	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "fma_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace(rendered)) {
		if err := os.WriteFile(
			filepath.Join(proofDirectory, fmaGoldenProofFile), []byte("executed\n"), 0o600,
		); err != nil {
			t.Fatalf("write live Python oracle proof: %v", err)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen FMA golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/fma_golden.json was " +
			"generated from production Python and frozen (release_impact._compute_confidence, " +
			"compute._percentile, compute_capacity._percentile, hotspots.compute_file_hotspots). " +
			"Regenerate with\n" +
			"    python tests/fixtures/generate_fma_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, change Go " +
			"too and re-verify every fma_golden_test.go bit-exact test; if it should not, the " +
			"Python change is the bug.",
	)
}
