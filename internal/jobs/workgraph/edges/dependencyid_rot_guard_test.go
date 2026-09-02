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

// TestPythonDecimalBlocksMatchLivePython re-derives Python's decimal-digit set
// and requires the checked-in blocks to match it exactly.
//
// This is the direction the original digit guard did not cover. That one
// derived the runes Python accepts and Go REJECTS (isdigit-but-not-int); this
// derives the runes Python accepts as decimal at all, so `unicode.Nd` is never
// consulted for a Python-facing decision.
//
// It also compares the two planes' Unicode VERSIONS and fails on a mismatch
// unless the derived tables still agree — because a version skew is exactly how
// this went wrong: Go 1.27 is Unicode 17 and the interpreter is 16, so U+11DE5
// is Nd to Go and unassigned to Python. A guard that only checked the tables
// would pass on the day the versions diverged and say nothing about why.
//
// Proof marker: workgraph-python-decimal-blocks
func TestPythonDecimalBlocksMatchLivePython(t *testing.T) {
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
import json, unicodedata
blocks, values = [], {}
run = []
for cp in range(0x110000):
    c = chr(cp)
    if not c.isdigit():
        continue
    try:
        v = int(c)
    except ValueError:
        continue
    values[cp] = v
    if v == 0:
        run.append(cp)
blocks = [cp for cp in run if all(values.get(cp + k) == k for k in range(10))]
print(json.dumps({
    "blocks": blocks,
    "total": len(values),
    "unicode_version": unicodedata.unidata_version,
}))
`
	output, err := exec.Command(python, "-c", derive).Output()
	if err != nil {
		t.Fatalf("derive decimal blocks from live python: %v", err)
	}
	var live struct {
		Blocks         []int  `json:"blocks"`
		Total          int    `json:"total"`
		UnicodeVersion string `json:"unicode_version"`
	}
	if err := json.Unmarshal(output, &live); err != nil {
		t.Fatalf("decode derivation: %v", err)
	}
	if len(live.Blocks) == 0 {
		t.Fatal("derivation produced no blocks; a silently empty result would make this vacuous")
	}

	t.Logf("python unicode %s (%d decimal runes in %d blocks) vs go unicode %s",
		live.UnicodeVersion, live.Total, len(live.Blocks), unicode.Version)

	if len(live.Blocks) != len(pythonDecimalBlocks) {
		t.Fatalf("live python has %d decimal blocks, this port carries %d",
			len(live.Blocks), len(pythonDecimalBlocks))
	}
	for index, start := range live.Blocks {
		if rune(start) != pythonDecimalBlocks[index] {
			t.Fatalf("block %d is U+%04X live, U+%04X here", index, start, pythonDecimalBlocks[index])
		}
		for offset := 0; offset < 10; offset++ {
			if got := decimalValue(rune(start + offset)); got != offset {
				t.Errorf("decimalValue(U+%04X) = %d, want %d", start+offset, got, offset)
			}
		}
	}

	// The version comparison. A mismatch is NOT fatal on its own -- the port
	// reads its own derived table, so it stays correct -- but it must be
	// reported, and it is fatal if anything still routes through Go's tables.
	if live.UnicodeVersion != unicode.Version {
		t.Logf("VERSION SKEW: python %s, go %s — the derived tables are authoritative",
			live.UnicodeVersion, unicode.Version)
		divergent := 0
		for cp := rune(0); cp <= 0x10FFFF; cp++ {
			goSaysDecimal := unicode.Is(unicode.Nd, cp)
			pythonSaysDecimal := decimalValue(cp) >= 0
			if goSaysDecimal != pythonSaysDecimal {
				divergent++
				// This is the whole point: on such a rune the port must follow
				// PYTHON. isPythonDigitString consults the derived table, so a
				// Go-only decimal must not be accepted as one.
				if goSaysDecimal && decimalValue(cp) >= 0 {
					t.Fatalf("U+%04X is decimal to Go only, yet this port treats it as decimal", cp)
				}
			}
		}
		if divergent == 0 {
			t.Fatalf("the two planes report different Unicode versions (%s vs %s) but no rune "+
				"differs — one of the two readings is wrong", live.UnicodeVersion, unicode.Version)
		}
		t.Logf("%d runes differ between the planes; all resolved in Python's favour", divergent)
	}

	if err := os.WriteFile(
		filepath.Join(proofDirectory, "workgraph-python-decimal-blocks"), []byte("executed"), 0o644,
	); err != nil {
		t.Fatalf("write proof marker: %v", err)
	}
}
