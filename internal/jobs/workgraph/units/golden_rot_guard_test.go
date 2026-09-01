package units

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkgraphComponentsGoldenMatchesLivePython is the rot guard for
// tests/fixtures/workgraph_components_split_python_golden.json (CHAOS-4441),
// following the pattern the repo_user_commit and numerical goldens established.
//
// TestBuildComponentsMatchesFrozenPythonGoldenExhaustively asserts that GO
// matches the frozen file. Nothing there asserts that PYTHON still does. That
// gap matters more here than for an ordinary metric: while the Go materializer
// is live and the Python membership projection is not yet ported, the two planes
// address rows in different tables by the same work_unit_id, and a change to
// components.py that Go does not follow re-addresses one plane and not the
// other -- silent divergence, no crash (backfill.py:113-127). Until CHAOS-4282
// lands and both jobs call one Go implementation, THIS TEST is the continuous
// guard on that window.
//
// A plain byte diff is sufficient: both sides are the same Python-rendered JSON
// text, and the generator is hermetic (it reads the frozen edge input rather
// than querying ClickHouse), so this runs anywhere the interpreter does.
func TestWorkgraphComponentsGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphComponentsLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_workgraph_components_python_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the golden generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(filepath.Join(repoRoot, "tests", "fixtures", goldenFixture))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "workgraph-components-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/" +
			"workgraph_components_split_python_golden.json was generated from the deployed\n" +
			"work_graph/investment/components.py and frozen. Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_workgraph_components_python_golden.py\n" +
			"and treat the diff as a REAL behaviour change with cross-plane consequences: a change\n" +
			"to component grouping or to work_unit_id re-addresses work_unit_investments and\n" +
			"work_unit_membership, which are written by two different jobs. Port the change into\n" +
			"internal/jobs/workgraph/units in the SAME change set, or the planes diverge silently.",
	)
}

// workgraphComponentsLivePython mirrors the repo_user_commit guard's helper:
// PYTHON wins, else python3 on PATH, and either way the resolved interpreter
// must resolve dev_health_ops to a module INSIDE this checkout -- otherwise the
// guard would silently compare another worktree's producer against this
// worktree's frozen golden.
func workgraphComponentsLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	command := exec.Command(
		resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)",
	)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := command.Output()
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
