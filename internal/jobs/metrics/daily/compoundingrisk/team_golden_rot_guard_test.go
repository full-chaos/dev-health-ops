package compoundingrisk

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestCompoundingRiskTeamGoldenMatchesLivePython is the rot guard for
// tests/fixtures/daily_compounding_risk_team_python_golden.json (CHAOS-5084),
// the team-scope sibling of TestCompoundingRiskGoldenMatchesLivePython. The
// frozen file was generated from REAL Python (_build_team_rows) and checked
// in; TestBuildTeamRowsMatchesFrozenPythonGolden asserts Go matches it, but
// nothing asserts Python STILL matches it. This re-runs the generator against
// the live interpreter and fails loudly, with a pointer to regenerate, the
// moment _build_team_rows (or the compute_compounding_risk it delegates to)
// changes its numbers out from under the frozen file.
func TestCompoundingRiskTeamGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := compoundingRiskRepositoryRoot(t)
	python := compoundingRiskLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_daily_compounding_risk_team_python_golden.py",
	)
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

	goldenPath := filepath.Join(
		repoRoot, "tests", "fixtures", "daily_compounding_risk_team_python_golden.json",
	)
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		// Distinct marker name from the repo-scope sibling's proof file: a
		// dropped or mistyped -run filter in ci/check_go.sh must not be able to
		// hide behind it.
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "compounding-risk-team-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Errorf(
		"live Python no longer reproduces the frozen team golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/daily_compounding_risk_team_python_golden.json " +
			"was generated from production Python and frozen. Regenerate with\n" +
			"    python tests/fixtures/generate_daily_compounding_risk_team_python_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, port the change " +
			"into compute.go's BuildTeamRows/MeanOrNone and update team_golden_test.go's teamGoldenCases too.",
	)
}
