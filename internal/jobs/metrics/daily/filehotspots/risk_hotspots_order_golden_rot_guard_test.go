package filehotspots

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRiskHotspotsOrderGoldenMatchesLivePython is the rot guard for
// tests/fixtures/risk_hotspots_order_golden.json (CHAOS-4863). Unlike the
// other rot guards in this package, this one does not merely re-run the
// generator and byte-compare -- the generator ITSELF re-verifies, at
// generation time, that live Python's risk_score is still order-invariant
// across CROSS_PROCESS_CONFIRMATIONS separate `python3` invocations per
// case (see generate_risk_hotspots_order_golden.py's module doc comment).
// A regenerate that finds Python has become order-DEPENDENT for some case
// would make the generator itself exit non-zero rather than silently
// produce a different (but still internally consistent) golden -- this
// guard would then fail with that error surfaced in its own output,
// which is exactly the "report the finding, do not retry" behavior CHAOS-
// 4863's ticket ruling asked for.
func TestRiskHotspotsOrderGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := fileHotspotsRepositoryRoot(t)
	python := fileHotspotsLivePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_risk_hotspots_order_golden.py")
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
		t.Fatalf("run the golden generator against live Python (may indicate Python's risk_score "+
			"became order-DEPENDENT -- read stderr, do not retry): %v: %s", err, stderr)
	}

	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "risk_hotspots_order_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "risk-hotspots-order-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Errorf(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/risk_hotspots_order_golden.json " +
			"was generated from production Python (hotspots.compute_file_risk_hotspots) and frozen. " +
			"Regenerate with\n" +
			"    python tests/fixtures/generate_risk_hotspots_order_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, re-verify " +
			"TestComputeFileRiskHotspotsOrderInvariantMatchesLivePythonBitExact too.",
	)
}
