package edges

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPythonLowerMatchesStrLowerOnTheKnownDivergences pins the two cases where
// Go's simple case mapping and Python's full case mapping disagree, in opposite
// directions. Both change which branch of CanonicalDependency a row takes.
func TestPythonLowerMatchesStrLowerOnTheKnownDivergences(t *testing.T) {
	// Measured against the live interpreter; the rot guard below re-derives.
	cases := map[string]struct{ input, python string }{
		"ascii":            {"BLOCKS", "blocks"},
		"turkish dotted I": {"İS_BLOCKED_BY", "i̇s_blocked_by"},
		"kelvin sign":      {"BLOCKS", "blocks"},
		"already lower":    {"is_blocked_by", "is_blocked_by"},
		"mixed":            {"Is_Blocked_By", "is_blocked_by"},
	}
	for name, testCase := range cases {
		if got := pythonLower(testCase.input); got != testCase.python {
			t.Errorf("%s: pythonLower(%q) = %q, python gives %q",
				name, testCase.input, got, testCase.python)
		}
	}

	// The point of the exercise: blocker-set membership must agree, because that
	// is what decides the branch.
	for name, testCase := range cases {
		_, goSays := blockerTypes[pythonLower(testCase.input)]
		_, pythonSays := blockerTypes[testCase.python]
		if goSays != pythonSays {
			t.Errorf("%s: blocker membership differs (go=%v python=%v) for %q",
				name, goSays, pythonSays, testCase.input)
		}
	}

	// And the two naive alternatives must each be WRONG on one of them, or this
	// function is solving a problem that does not exist.
	if strings.ToLower("İS_BLOCKED_BY") == "i̇s_blocked_by" {
		t.Error("strings.ToLower now matches Python on U+0130; pythonLower may be unnecessary")
	}
	if _, wrong := blockerTypes[strings.ToLower("İS_BLOCKED_BY")]; !wrong {
		t.Error("strings.ToLower no longer folds U+0130 into the blocker set; the premise changed")
	}
}

// TestPythonLowerMatchesLivePython re-derives the set of runes whose lowercase
// mapping is longer than one rune — the entire difference between Go's simple
// mapping and Python's full mapping — and requires pythonLower to handle exactly
// those.
//
// Hard-coding U+0130 is correct today and is the kind of constant that rots: a
// Unicode revision adding another multi-char lowercase mapping would silently
// reintroduce the divergence in whichever direction the new rune falls.
func TestPythonLowerMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		resolved, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required: %v", err)
		}
		python = resolved
	}
	const derive = `
import json, sys
out = {}
for cp in range(0x110000):
    c = chr(cp)
    low = c.lower()
    if len(low) > 1:
        out[cp] = low
json.dump(out, sys.stdout)
`
	command := exec.Command(python, "-c", derive)
	rendered, err := command.Output()
	if err != nil {
		var stderr []byte
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		t.Fatalf("derive multi-rune lowercase mappings: %v: %s", err, stderr)
	}
	var multi map[string]string
	if err := json.Unmarshal(rendered, &multi); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(multi) == 0 {
		t.Fatal("live Python reports no multi-rune lowercase mappings; the derivation is broken")
	}

	// Every one of them must round-trip through pythonLower to what Python gives.
	// A rune this function does not special-case will fail here rather than in
	// production, which is the whole point.
	for codePoint, expected := range multi {
		var value rune
		if _, err := parseCodePoint(codePoint, &value); err != nil {
			t.Fatalf("bad code point %q: %v", codePoint, err)
		}
		if got := pythonLower(string(value)); got != expected {
			t.Errorf(
				"U+%04X lowercases to %q in Python but %q here; pythonLower does not handle it, "+
					"so a relationship_type containing it would take a different branch than Python",
				value, expected, got,
			)
		}
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-python-lower"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof: %v", err)
	}
	t.Logf("pythonLower matches live str.lower() on all %d multi-rune mappings", len(multi))
}

func parseCodePoint(value string, into *rune) (rune, error) {
	number := 0
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, errors.New("not a decimal code point")
		}
		number = number*10 + int(r-'0')
	}
	*into = rune(number)
	return *into, nil
}
