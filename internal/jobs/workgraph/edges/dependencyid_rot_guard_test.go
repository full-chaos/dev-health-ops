package edges

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// TestNumericTypeDigitTableMatchesLivePython re-derives numericTypeDigitNotDecimal
// from the deployed interpreter and requires the checked-in table to match.
//
// That table is a hand-carried copy of a Unicode property Go does not expose, so
// it is exactly the kind of constant that rots silently: a Python upgrade adding
// a Numeric_Type=Digit character would widen `str.isdigit()`, widening the set
// that reaches `int()` and crashes — and nothing else in this package would
// notice. The parity claim in ParsePRDependencySource is only as good as this.
//
// The derivation is the definition, not an approximation of it: every rune where
// `isdigit()` is True and `int()` raises.
func TestNumericTypeDigitTableMatchesLivePython(t *testing.T) {
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
out = []
for cp in range(0x110000):
    c = chr(cp)
    if not c.isdigit():
        continue
    try:
        int(c)
    except ValueError:
        out.append(cp)
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
		t.Fatalf("derive the Numeric_Type=Digit set from live Python: %v: %s", err, stderr)
	}
	var live []rune
	if err := json.Unmarshal(rendered, &live); err != nil {
		t.Fatalf("decode derived set: %v", err)
	}
	if len(live) == 0 {
		t.Fatal("live Python derived an empty set; the derivation is broken, not the table")
	}

	expected := map[rune]struct{}{}
	for _, r := range live {
		expected[r] = struct{}{}
	}
	missing, extra := 0, 0
	for r := range expected {
		if !unicode.Is(numericTypeDigitNotDecimal, r) {
			if missing == 0 {
				t.Errorf("checked-in table is MISSING U+%04X (and possibly more): live Python "+
					"accepts it in isdigit() but int() rejects it, so it would crash the Python "+
					"builder while this port skipped it silently", r)
			}
			missing++
		}
	}
	for r := rune(0); r < 0x110000; r++ {
		if !unicode.Is(numericTypeDigitNotDecimal, r) {
			continue
		}
		if _, ok := expected[r]; !ok {
			if extra == 0 {
				t.Errorf("checked-in table has EXTRA U+%04X (and possibly more): this port would "+
					"reject it as malformed where Python parses it", r)
			}
			extra++
		}
	}
	if missing != 0 || extra != 0 {
		t.Fatalf("table drift: %d missing, %d extra (live set has %d runes)", missing, extra, len(live))
	}

	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-numeric-digit-table"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write live-python-oracle proof: %v", err)
	}
	t.Logf("table matches live Python: %d runes", len(live))
}

// TestIntMaxStrDigitsMatchesLivePython reads the limit back from the deployed
// interpreter rather than trusting the constant.
//
// It is a SETTING, not a language constant — `sys.set_int_max_str_digits()`
// changes it at runtime and it did not exist before 3.11 — so a deployment that
// raised or lowered it would leave this port disagreeing with the reference
// about which ids are convertible, silently, in the direction that mislabels a
// crashing row as an ordinary PR.
//
// Proof marker: workgraph-int-max-str-digits
func TestIntMaxStrDigitsMatchesLivePython(t *testing.T) {
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
	output, err := exec.Command(python, "-c", "import sys; print(sys.get_int_max_str_digits())").Output()
	if err != nil {
		t.Fatalf("read int_max_str_digits from live python: %v", err)
	}
	live, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		t.Fatalf("parse int_max_str_digits %q: %v", output, err)
	}
	if live != pythonIntMaxStrDigits {
		t.Fatalf("live python allows %d digits, this port models %d — ids between the two "+
			"are convertible for one and a crash for the other", live, pythonIntMaxStrDigits)
	}
	// The boundary itself, executed on both planes rather than reasoned about.
	if _, _, _, ok := pythonIntFromDigits(strings.Repeat("9", live)); !ok {
		t.Errorf("%d digits must convert; python accepts exactly this many", live)
	}
	if _, _, _, ok := pythonIntFromDigits(strings.Repeat("9", live+1)); ok {
		t.Errorf("%d digits must NOT convert; python raises", live+1)
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-int-max-str-digits"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}
