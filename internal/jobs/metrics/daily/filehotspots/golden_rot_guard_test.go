package filehotspots

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestFileHotspotsGoldenMatchesLivePython is the rot guard for
// tests/fixtures/file_hotspots_python_golden.json (CHAOS-4277), following the
// exact pattern internal/jobs/metrics/daily/repouser's
// golden_rot_guard_test.go established: the frozen file was generated from
// REAL production Python and checked in; TestComputeMatchesFrozenPythonGolden
// (golden_full_test.go) asserts Go matches it, but nothing asserts Python
// STILL matches it. This re-runs the generator against the live interpreter
// and fails loudly, with a pointer to regenerate, the moment
// compute_file_hotspots/compute_file_risk_hotspots changes its numbers out
// from under the frozen file.
func TestFileHotspotsGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := fileHotspotsRepositoryRoot(t)
	python := fileHotspotsLivePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_file_hotspots_python_golden.py")
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

	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "file_hotspots_python_golden.json")
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "file-hotspots-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Errorf(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/file_hotspots_python_golden.json " +
			"was generated from production Python and frozen. Regenerate with\n" +
			"    python tests/fixtures/generate_file_hotspots_python_golden.py\n" +
			"and review the diff as a real behaviour change -- if Go should follow, port the change " +
			"into compute.go and update golden_full_test.go's hardcoded expectations too.",
	)
}

func fileHotspotsRepositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

// fileHotspotsLivePython mirrors repouser's identically-named helper: PYTHON
// env var wins, else python3 on PATH, and either way the resolved
// interpreter must resolve dev_health_ops to a module INSIDE this checkout --
// otherwise the guard would silently compare another worktree's producer
// against this worktree's frozen golden.
func fileHotspotsLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	cmd := exec.Command(resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := cmd.Output()
	if err != nil {
		t.Fatalf("resolve dev_health_ops with %s: %v", resolved, err)
	}
	module := string(located)
	if !strings.HasPrefix(module, repoRoot+string(os.PathSeparator)) {
		t.Fatalf(
			"%s resolves dev_health_ops to %s, which is OUTSIDE this checkout (%s) -- "+
				"the guard would be comparing another worktree's producer against this "+
				"worktree's frozen golden; set PYTHONPATH to this checkout's src",
			resolved, module, repoRoot,
		)
	}
	return resolved
}
