package repouser

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestPysumGoldenMatchesLivePython is the CHAOS-4824 rot guard for
// tests/fixtures/pysum_golden.json, in the same shape as the sibling
// CHAOS-4818 rot guards this session added after codex (round 4 on #2106)
// found a generator that imports and calls live production Python but is
// never actually invoked by CI -- a frozen fixture with no regeneration
// guard measures history, not parity, and degrades silently the moment the
// Python reference changes.
func TestPysumGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		if resolved, err := exec.LookPath("python3"); err == nil {
			python = resolved
		} else {
			t.Fatalf("PYTHON is required for the live pysum oracle: %v", err)
		}
	}

	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	generator := filepath.Join(root, "tests", "fixtures", "generate_pysum_golden.py")
	if info, statErr := os.Stat(generator); statErr != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, statErr)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run the pysum golden generator against live Python: %v\nstderr:\n%s", err, stderr.String())
	}
	rendered := stdout.Bytes()

	goldenPath := filepath.Join(root, "tests", "fixtures", "pysum_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace(rendered)) {
		if writeErr := os.WriteFile(filepath.Join(proofDirectory, "pysum-golden"), []byte("executed"), 0o600); writeErr != nil {
			t.Fatalf("write live Python oracle proof: %v", writeErr)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen pysum golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/pysum_golden.json was " +
			"generated from production compute_code_ownership_gini/compute_pipeline_stability " +
			"and frozen. Regenerate with\n" +
			"    python tests/fixtures/generate_pysum_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, change Go " +
			"too and re-verify TestCodeOwnershipGiniMatchesLivePythonBitExact; if it should not, " +
			"the Python change is the bug.",
	)
}
