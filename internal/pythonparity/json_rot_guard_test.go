package pythonparity

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPythonJSONGoldenMatchesLivePython is the rot guard for
// tests/fixtures/python_json_python_golden.json (CHAOS-4441), following the
// pattern established by the workgraph-components and confidence-coercion
// guards.
//
// TestMarshalPythonJSONMatchesCPython asserts that GO still matches the frozen
// file. Nothing there asserts that PYTHON still does. That gap is what this
// closes, and it matters for a specific reason: the frozen file is the only
// statement anywhere that CPython's json.dumps produces these bytes. If a
// Python-side change altered the payload -- a new key in input_payload, a
// changed truncation limit, a different source ordering -- the Go tests would
// go on passing against a golden that no longer describes production, and the
// first symptom would be a categorization_input_hash that matches no stored
// row: every work unit re-categorizing on every run, silently, at real LLM
// cost.
//
// The diff is over the WHOLE fixture, which means the `bundle_cases` section is
// guarded here even though no Go test consumes it yet. That is deliberate:
// build_text_bundle's own logic (its caps, its empty-text filtering, its handle
// numbering) is pinned against Python drift from the moment the fixture exists,
// rather than from whenever the Go port of that function lands.
func TestPythonJSONGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := pythonJSONRepositoryRoot(t)
	python := pythonJSONLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_python_json_golden.py",
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

	frozen, err := os.ReadFile(filepath.Join(
		repoRoot, "tests", "fixtures", "python_json_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}

	if string(frozen) == string(rendered) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "python-json-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug: tests/fixtures/python_json_python_golden.json\n" +
			"was generated from the deployed work_graph/investment/evidence.py and frozen.\n" +
			"Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_python_json_golden.py\n" +
			"and treat the diff as a REAL behaviour change with a direct cost consequence:\n" +
			"input_hash is categorization_input_hash, which is the LLM skip-existing key\n" +
			"(materialize.py: WHERE categorization_input_hash IN %(input_hashes)s). A changed\n" +
			"hash matches no stored row, so EVERY work unit re-categorizes on EVERY run --\n" +
			"a full re-bill, with no error and no zero-row alarm to notice it by. Port the\n" +
			"change into internal/pythonparity in the SAME change set.",
	)
}

// pythonJSONRepositoryRoot walks up to the module root.
func pythonJSONRepositoryRoot(t *testing.T) string {
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

// pythonJSONLivePython mirrors the workgraph-components guard's helper: PYTHON
// wins, else python3 on PATH, and either way the resolved interpreter must
// resolve dev_health_ops to a module INSIDE this checkout -- otherwise the
// guard would silently compare another worktree's producer against this
// worktree's frozen golden, which is a green that means nothing.
func pythonJSONLivePython(t *testing.T, repoRoot string) string {
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
