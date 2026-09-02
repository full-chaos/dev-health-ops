package pythonparity

import (
	"os"
	"os/exec"
	"path/filepath"
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

	repoRoot := parityRepositoryRoot(t)
	python := parityLivePython(t, repoRoot)
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
