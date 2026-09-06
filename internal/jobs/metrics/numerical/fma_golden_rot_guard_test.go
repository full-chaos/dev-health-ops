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
// compute._percentile, compute_capacity._percentile) and then frozen. The
// bit-exact tests in fma_golden_test.go (and its per-package copies) only
// prove Go matches the FILE -- nothing proves Python still matches the file,
// so the moment any of those functions' numerics change, the frozen
// `expected_bits` values silently encode the OLD Python behaviour and every
// bit-exact test stays green while the two implementations have actually
// diverged.
//
// CHAOS-5234/CHAOS-3092 (2026-09-06): fma_golden.json used to carry a fourth
// family, hotspot_score (dev_health_ops.metrics.hotspots.compute_file_hotspots),
// which this generator regenerated too -- deleted now that file_hotspots is
// fully native (no straddle). Rather than special-case that one key out of
// this comparison, its frozen cases were split VERBATIM into a standalone
// tests/fixtures/fma_hotspot_score_golden.json with no generator (see
// filehotspots/fma_golden_test.go, which reads it directly): this file's own
// generator, frozen output, and this plain byte-equality check all stay in
// their original, simpler shape.
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
			"compute._percentile, compute_capacity._percentile). Regenerate with\n" +
			"    python tests/fixtures/generate_fma_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, change Go " +
			"too and re-verify every fma_golden_test.go bit-exact test; if it should not, the " +
			"Python change is the bug.",
	)
}
