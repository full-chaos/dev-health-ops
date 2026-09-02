package filehotspots

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestFMAFollowupGoldenMatchesLivePython is the rot guard for
// tests/fixtures/fma_followup_golden.json, the AST-lint follow-up's own
// golden (hotspot_risk_score family here; ownership_gini family consumed by
// repouser's TestCodeOwnershipGiniFMAFollowupMatchesLivePythonBitExact).
// One rot guard test suffices for the whole file regardless of how many
// packages consume it -- see internal/jobs/metrics/numerical's
// fma_golden_rot_guard_test.go for the same one-guard-per-golden-file
// convention with fma_golden.json (which is likewise consumed by more than
// one package). Reuses this package's own fileHotspotsRepositoryRoot/
// fileHotspotsLivePython helpers (golden_rot_guard_test.go) rather than
// redefining them.
func TestFMAFollowupGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := fileHotspotsRepositoryRoot(t)
	python := fileHotspotsLivePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_fma_followup_golden.py")
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	cmd := exec.Command(python, generator, "--stdout")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the golden generator against live Python: %v: %s", err, stderr)
	}

	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "fma_followup_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "fma-followup-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Errorf(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/fma_followup_golden.json " +
			"was generated from production Python (hotspots.compute_file_risk_hotspots, " +
			"knowledge.compute_code_ownership_gini) and frozen. Regenerate with\n" +
			"    python tests/fixtures/generate_fma_followup_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, port the " +
			"change and re-verify TestSampleZScoresMatchesLivePythonBitExact and " +
			"TestCodeOwnershipGiniFMAFollowupMatchesLivePythonBitExact too.",
	)
}
