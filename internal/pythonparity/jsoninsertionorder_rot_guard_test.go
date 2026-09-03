package pythonparity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestPythonJSONInsertionOrderGoldenMatchesLivePython is the rot guard for
// tests/fixtures/python_json_insertion_order_python_golden.json.
//
// TestMarshalPythonJSONInsertionOrderMatchesCPython asserts that GO still
// matches the frozen file. Nothing there asserts that PYTHON still does. That
// gap is what this closes.
//
// It matters more here than for a pure-function golden, because this fixture
// records a DEFAULT-ARGUMENT contract rather than a computation. Everything it
// pins is something CPython chose and could in principle re-choose:
//
//	insertion order    dicts have preserved it since 3.7; json.dumps follows
//	allow_nan=True     bare Infinity/-Infinity/NaN, which are not valid JSON
//	ensure_ascii=True  non-ASCII escaped, astral as surrogate pairs
//	separators         ", " and ": " when indent is None
//	float rendering    float.__repr__, not str() and not %g
//
// A change to any of those is invisible to the Go tests: they would keep
// agreeing with a frozen file that no longer describes what Python does. The
// interpreter version is recorded in the fixture for the same reason -- the
// float repr algorithm is a property of the interpreter, not of this repo.
func TestPythonJSONInsertionOrderGoldenMatchesLivePython(t *testing.T) {
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
		repoRoot, "tests", "fixtures",
		"generate_python_json_insertion_order_golden.py",
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
		repoRoot, "tests", "fixtures",
		insertionOrderGuard.fixture,
	))
	if err != nil {
		t.Fatal(err)
	}

	// Compare the PAYLOAD, not the whole document: provenance fields such as
	// `environment` record an interpreter that drifts without anyone deciding
	// anything, and freezing that inside the comparison already produced one
	// false "has ROTTED" pointing at loader.py. See comparePayload.
	if err := comparePayload(frozen, rendered, insertionOrderGuard.fields...); err == nil {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "python-json-insertion-order-golden"),
			[]byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen golden.\n" +
			"This is Python drift, not a Go bug:\n" +
			"tests/fixtures/python_json_insertion_order_python_golden.json was generated\n" +
			"from live CPython and frozen. Regenerate with\n" +
			"    PYTHONPATH=src python tests/fixtures/generate_python_json_insertion_order_golden.py\n" +
			"and read the diff carefully before accepting it. This fixture pins a\n" +
			"DEFAULT-ARGUMENT contract -- insertion order, allow_nan=True's bare\n" +
			"Infinity/-Infinity/NaN tokens, ensure_ascii, the \", \"/\": \" separators and\n" +
			"float.__repr__ rendering. A diff here means one of those defaults changed\n" +
			"underneath every caller, including recommendations/loader.py:448, which\n" +
			"writes the evidence_json column with a bare json.dumps(evidence_list).\n" +
			"Port the change into internal/pythonparity in the SAME change set.",
	)
}
