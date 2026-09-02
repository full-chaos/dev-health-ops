package pythonparity

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestFloatTextGoldenMatchesLivePython is the rot guard for
// testdata/float_text_golden.json.
//
// The golden tests assert that GO matches the frozen file. Nothing in them
// asserts that PYTHON still does. That gap is the whole risk here, because
// every behaviour this package mirrors is an interpreter implementation
// detail with no wire format and no diff in src/ when it moves:
// round()'s dtoa-based banker's rounding, repr()'s notation window, and the
// fixed-point format spec. A CPython upgrade can change any of them with
// nothing in this repository changing at all.
//
// The guard regenerates the corpus with the deployed interpreter and compares
// it byte for byte against the committed file.
func TestFloatTextGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repositoryRoot := floatTextRepositoryRoot(t)
	generator := filepath.Join(
		repositoryRoot, "internal", "pythonparity", "testdata", "generate_float_text_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	command := exec.Command(floatTextLivePython(t, repositoryRoot), generator)
	command.Dir = repositoryRoot
	regenerated, err := command.Output()
	if err != nil {
		t.Fatalf("regenerate golden with the live interpreter: %v", err)
	}

	committed, err := os.ReadFile(filepath.Join(repositoryRoot,
		"internal", "pythonparity", "testdata", "float_text_golden.json"))
	if err != nil {
		t.Fatalf("read committed golden: %v", err)
	}

	// Compare the DATA and the RECORDED ENVIRONMENT separately, because they
	// answer different questions and only one of them is under test.
	//
	// The data is what the Go mirrors are asserted against. The environment
	// block exists to ATTRIBUTE a data mismatch, not to be one: this corpus is
	// float formatting and rounding, which is IEEE-754 and exact-dtoa, so it is
	// architecture-independent by construction -- unlike arithmetic, where
	// arm64 FMA genuinely diverges (CHAOS-4818).
	//
	// An earlier version compared the whole document byte-for-byte and then
	// attributed any difference to the interpreter. CI runs x86_64 while this
	// corpus was generated on arm64, so `machine` and `maxsize` differ while
	// every value is identical -- and the guard failed with "THE INTERPRETER
	// MOVED", which was simply untrue: same CPython 3.14.7, same Unicode
	// 16.0.0, different CPU. A guard that misnames its cause sends the reader
	// to the wrong remedy, which for this one is "regenerate the fixture" --
	// the harmful action.
	committedData, committedEnvironment := splitFloatTextDocument(t, committed, "committed")
	liveData, liveEnvironment := splitFloatTextDocument(t, regenerated, "regenerated")

	if string(committedData) == string(liveData) {
		if committedEnvironment != liveEnvironment {
			// Worth saying out loud rather than passing silently: the corpus
			// was produced somewhere else and still reproduces exactly, which
			// is evidence FOR architecture independence, not a problem.
			t.Logf("float-text golden reproduces exactly on a different environment "+
				"(committed: %s; here: %s) -- expected, this corpus is architecture-independent",
				committedEnvironment, liveEnvironment)
		} else {
			t.Logf("float-text golden matches the live interpreter: %s", liveEnvironment)
		}
		writeFloatTextProof(t, proofDirectory)
		return
	}

	// The two causes of a mismatch have OPPOSITE correct remedies, and a bare
	// diff points a reader at the wrong one. Naming which fired is the whole
	// value of this guard over a plain byte comparison.
	committedInterpreter := floatTextInterpreterOf(t, committed, "committed")
	liveInterpreter := floatTextInterpreterOf(t, regenerated, "regenerated")

	// Reached only when the DATA differs. The environment block now explains
	// that difference rather than causing it.
	// Name what ACTUALLY differs. An architecture change reads as an
	// "interpreter" change if the identity is compared as one opaque string,
	// and the two call for different responses: a moved interpreter may
	// justify regenerating, a moved CPU never does for this corpus.
	if committedEnvironment != liveEnvironment && floatTextInterpreterCore(t, committed) == floatTextInterpreterCore(t, regenerated) {
		t.Fatalf(`CAUSE: THE DATA DIFFERS, AND SO DOES THE ARCHITECTURE -- BUT THE INTERPRETER IS THE SAME.

Committed: %s
Here:      %s

The CPython version, implementation and Unicode version are IDENTICAL; only the
machine differs. This corpus is float formatting and rounding -- IEEE-754 and
exact dtoa -- which is architecture-independent, so an architecture difference
CANNOT explain a data difference here. Something else changed.

Do NOT regenerate. Find the real cause: a hand-edited fixture, a generator
change that was not regenerated, or a genuine architecture-dependent behaviour
this corpus was not expected to contain (which would itself be the finding --
compare against CHAOS-4818, where arm64 FMA does diverge, but for ARITHMETIC,
not formatting).`, committedEnvironment, liveEnvironment)
	}

	if committedInterpreter != liveInterpreter {
		t.Fatalf(`CAUSE: THE INTERPRETER MOVED, NOT THE FIXTURE.

The golden was generated by %s and this environment runs %s.

Do NOT simply regenerate the fixture. Regenerating makes the Go mirrors agree
with THIS interpreter, which is only correct if this interpreter is the one
production ships. Establish that first:

  * if this environment is the outlier (a CI image pinned behind the shipped
    interpreter -- see the go.yml 3.13-vs-3.14 skew, CHAOS-4441), fix the pin
    here and leave the fixture alone;
  * only if the SHIPPED interpreter has genuinely moved should the fixture be
    regenerated -- and then Round/Repr/FormatFixed must be re-read against the
    new CPython behaviour, because a regenerated fixture that the Go code still
    passes may simply mean the change is invisible to this corpus.`,
			committedInterpreter, liveInterpreter)
	}

	t.Fatalf(`CAUSE: SAME INTERPRETER (%s), DIFFERENT OUTPUT -- THE FIXTURE IS STALE.

The committed golden no longer matches what the generator produces on the very
interpreter that produced it, so the generator was edited without regenerating,
or the fixture was hand-edited. Re-run:

  .venv/bin/python internal/pythonparity/testdata/generate_float_text_golden.py \
    > internal/pythonparity/testdata/float_text_golden.json

and review the diff before committing it.`, liveInterpreter)
}

// floatTextInterpreterOf extracts the recorded generating-interpreter identity
// so a mismatch can be attributed rather than merely reported.
func floatTextInterpreterOf(t *testing.T, document []byte, label string) string {
	t.Helper()
	var parsed struct {
		GeneratingInterpreter struct {
			PythonVersion  string `json:"python_version"`
			Implementation string `json:"implementation"`
			UnicodeVersion string `json:"unicode_version"`
			Machine        string `json:"machine"`
			FloatReprStyle string `json:"float_repr_style"`
		} `json:"generating_interpreter"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		t.Fatalf("decode %s golden: %v", label, err)
	}
	interpreter := parsed.GeneratingInterpreter
	if interpreter.PythonVersion == "" {
		t.Fatalf("%s golden records no generating interpreter; "+
			"a fixture that does not record what produced it cannot be attributed", label)
	}
	return interpreter.Implementation + " " + interpreter.PythonVersion +
		" (Unicode " + interpreter.UnicodeVersion +
		", float_repr_style " + interpreter.FloatReprStyle +
		", " + interpreter.Machine + ")"
}

// floatTextLivePython resolves the project interpreter, preferring the
// worktree venv over whatever `python3` PATH happens to offer.
//
// ci/check_go.sh's live-oracle verb shells out to a bare `python3`, and a
// system interpreter without the project's dependencies reports failures that
// read like divergences rather than like a missing environment. This generator
// imports only the standard library, so a bare python3 would in fact work --
// but it might be a DIFFERENT VERSION from the one the project ships, which
// for this particular corpus is precisely the thing under test. Preferring the
// venv keeps the guard measuring the shipped interpreter.
func floatTextLivePython(t *testing.T, repositoryRoot string) string {
	t.Helper()
	venvPython := filepath.Join(repositoryRoot, ".venv", "bin", "python")
	if info, err := os.Stat(venvPython); err == nil && !info.IsDir() {
		return venvPython
	}
	resolved, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("no project venv at %s and no python3 on PATH: %v", venvPython, err)
	}
	return resolved
}

func floatTextRepositoryRoot(t *testing.T) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return strings.TrimSpace(string(output))
}

// writeFloatTextProof records that the comparison actually ran. A guard whose
// only output is a silent pass is indistinguishable from a guard that was
// skipped, which is the failure mode ci/check_go.sh's proof directory exists
// to close: the shell verb re-reads this marker after the test process exits
// and fails the gate when it is absent.
//
// The contents are the literal "executed" the harness compares against; the
// interpreter identity goes to the test log, which is where a human reads it.
func writeFloatTextProof(t *testing.T, proofDirectory string) {
	t.Helper()
	proof := filepath.Join(proofDirectory, "pythonparity-float-text")
	if err := os.WriteFile(proof, []byte("executed"), 0o644); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}

// splitFloatTextDocument separates the corpus DATA from the recorded
// environment, so a mismatch in one is never reported as a mismatch in the
// other.
//
// Returns the data re-serialised with TOP-LEVEL KEY ORDER normalised, and
// nothing else. Be precise about the limit, because overclaiming it would
// recreate the very misattribution this file exists to prevent.
//
// json.Marshal sorts map keys, so the order of "rounds"/"reprs"/"formats" is
// canonical. It does NOT normalise anything inside them: the values are
// json.RawMessage and their bytes are copied verbatim, so whitespace, indent
// and key order WITHIN each entry are whatever the source document had.
//
// The consequence to know: if the committed JSON is ever reformatted -- a
// linter, a different indent, a regeneration with different json.dumps
// arguments -- the data comparison fails and this guard reports "SAME
// INTERPRETER, DIFFERENT OUTPUT", blaming the generator when the cause was
// spacing. That is not a hypothetical shape; measured, the same data at two
// spacings is 2,765,074 vs 3,837,964 bytes.
//
// It is unreachable today because one generator with one fixed json.dumps call
// produces both sides. That is an assumption about the PRODUCER, not a property
// of this function, and it is the assumption to re-check if the generator's
// serialisation ever changes.
func splitFloatTextDocument(t *testing.T, document []byte, label string) (data []byte, environment string) {
	t.Helper()
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(document, &parsed); err != nil {
		t.Fatalf("decode %s golden: %v", label, err)
	}
	environment = floatTextInterpreterOf(t, document, label)
	delete(parsed, "generating_interpreter")

	canonical, err := json.Marshal(parsed)
	if err != nil {
		t.Fatalf("re-serialise %s golden data: %v", label, err)
	}
	return canonical, environment
}

// floatTextInterpreterCore returns only the fields that can change CPython's
// float BEHAVIOUR, deliberately excluding the machine, which cannot for this
// corpus. Keeping them separate is what lets the guard say "the CPU moved"
// rather than blaming the interpreter.
//
// float_repr_style is in here because it is behaviour-determining in the
// strongest sense, and its absence was a real hole (CHAOS-4870, found by
// lane-4441). The third verdict below asserts that architecture cannot explain
// a data difference, on the grounds that formatting is IEEE-754 and exact dtoa.
// That premise holds only while CPython uses its OWN dtoa: a build with
// PY_NO_SHORT_FLOAT_REPR reports float_repr_style == "legacy" and defers
// repr/format to the PLATFORM LIBC, at which point formatting genuinely is
// platform-dependent and the premise is false. The fixture recorded that field
// from the start -- and nothing read it, so it could move freely while the
// guard kept asserting a premise it had invalidated.
func floatTextInterpreterCore(t *testing.T, document []byte) string {
	t.Helper()
	var parsed struct {
		GeneratingInterpreter struct {
			PythonVersion  string `json:"python_version"`
			Implementation string `json:"implementation"`
			UnicodeVersion string `json:"unicode_version"`
			FloatReprStyle string `json:"float_repr_style"`
		} `json:"generating_interpreter"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		t.Fatalf("decode interpreter core: %v", err)
	}
	interpreter := parsed.GeneratingInterpreter
	return interpreter.Implementation + " " + interpreter.PythonVersion +
		" (Unicode " + interpreter.UnicodeVersion +
		", float_repr_style " + interpreter.FloatReprStyle + ")"
}
