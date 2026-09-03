package textrefs

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

// TestPythonDigitValueMatchesLivePythonForEveryDigit checks pythonDigitValue
// against int() for EVERY rune the live interpreter treats as `\d`.
//
// The implementation assumes each Nd block is ten consecutive code points and
// walks back to the block start. That is true by Unicode's own rules, but two
// Nd blocks being ADJACENT would let the walk cross a boundary and return a
// wrong value -- so it is checked exhaustively rather than argued.
func TestPythonDigitValueMatchesLivePythonForEveryDigit(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
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
import json, re
out = {}
for cp in range(0x110000):
    c = chr(cp)
    if re.match(r"\d", c):
        out[cp] = int(c)
print(json.dumps(out))
`
	output, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive digit values from live python: %v", err)
	}
	var want map[string]int
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode digit values: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("live python produced no digits; this comparison would be vacuous")
	}

	mismatches := 0
	for key, expected := range want {
		var cp rune
		n, err := strconv.Atoi(key)
		if err != nil {
			t.Fatalf("parse code point %q: %v", key, err)
		}
		cp = rune(n)
		got, ok := pythonDigitValue(cp)
		if !ok {
			t.Errorf("U+%04X: live Python says \\d with value %d, pythonDigitValue says not a digit",
				cp, expected)
			mismatches++
		} else if got != expected {
			t.Errorf("U+%04X: value %d, want %d (block-start walk crossed a boundary?)",
				cp, got, expected)
			mismatches++
		}
		if mismatches > 20 {
			t.Fatal("too many mismatches; stopping")
		}
	}
	t.Logf("checked %d digit runes from live Python", len(want))
}
