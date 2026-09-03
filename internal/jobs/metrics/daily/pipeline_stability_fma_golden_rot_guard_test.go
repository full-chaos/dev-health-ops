package daily

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestPipelineStabilityFMAGoldenMatchesLivePython is the CHAOS-4818 rot
// guard for tests/fixtures/pipeline_stability_fma_golden.json (site 10,
// codex round 3 on PR #2106), in the same shape as
// internal/jobs/metrics/numerical's TestFMAGoldenMatchesLivePython.
//
// Codex round 4 (P2) found this fixture had a generator that imports and
// calls live production Python (generate_pipeline_stability_fma_golden.py)
// but nothing invoked it -- the Go test only ever read the frozen JSON, so
// a future change to compute_pipeline_stability could leave the frozen
// `expected` bits encoding the OLD Python behaviour while this package's
// bit-pattern test kept comparing Go only against that stale file, staying
// green with no signal that Python had moved.
func TestPipelineStabilityFMAGoldenMatchesLivePython(t *testing.T) {
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
			t.Fatalf("PYTHON is required for the live pipeline-stability FMA oracle: %v", err)
		}
	}

	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	generator := filepath.Join(root, "tests", "fixtures", "generate_pipeline_stability_fma_golden.py")
	if info, statErr := os.Stat(generator); statErr != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, statErr)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run the pipeline-stability FMA golden generator against live Python: %v\nstderr:\n%s", err, stderr.String())
	}
	rendered := stdout.Bytes()

	goldenPath := filepath.Join(root, "tests", "fixtures", "pipeline_stability_fma_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace(rendered)) {
		if writeErr := os.WriteFile(filepath.Join(proofDirectory, "pipeline-stability-fma-golden"), []byte("executed"), 0o600); writeErr != nil {
			t.Fatalf("write live Python oracle proof: %v", writeErr)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen pipeline-stability FMA golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/pipeline_stability_fma_golden.json " +
			"was generated from production compute_pipeline_stability and frozen. Regenerate with\n" +
			"    python tests/fixtures/generate_pipeline_stability_fma_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, change Go too " +
			"and re-verify TestComputePipelineStabilityMatchesLivePythonBitExact; if it should not, " +
			"the Python change is the bug.",
	)
}
