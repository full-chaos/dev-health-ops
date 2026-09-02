package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

type decimalDigitsGolden struct {
	UnidataVersion         string  `json:"unidata_version"`
	IntMaxStrDigits        int     `json:"int_max_str_digits"`
	DecimalDigits          [][]int `json:"decimal_digits"`
	DigitButNotDecimal     []int   `json:"digit_but_not_decimal"`
	IntVsIsDecimalMismatch []any   `json:"int_vs_isdecimal_mismatches"`
}

func loadDecimalDigitsGolden(t *testing.T) decimalDigitsGolden {
	t.Helper()
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures",
		"python_decimal_digits_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read decimal-digits golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_python_decimal_digits_golden.py)", err)
	}
	var golden decimalDigitsGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse decimal-digits golden: %v", err)
	}
	if len(golden.DecimalDigits) == 0 {
		t.Fatal("golden lists no decimal digits")
	}
	return golden
}

// TestDecimalDigitValueMatchesTheInterpreter verifies the digit-value
// derivation against every code point CPython calls decimal, rather than
// against Go's own tables.
//
// The derivation walks back to the start of the Nd run, and an off-by-one there
// returns a value uniformly one too high -- which still parses and still looks
// like a number. parsePythonInt("١٢") returned 23 instead of 12 before this
// test existed, and nothing else in the suite would have noticed, because the
// result is a plausible integer.
func TestDecimalDigitValueMatchesTheInterpreter(t *testing.T) {
	golden := loadDecimalDigitsGolden(t)

	if n := len(golden.IntVsIsDecimalMismatch); n != 0 {
		t.Errorf("the interpreter reports %d code points where int() and "+
			"str.isdecimal() disagree; this port assumes they are the same set", n)
	}

	var mismatches int
	for _, pair := range golden.DecimalDigits {
		if len(pair) != 2 {
			t.Fatalf("malformed [code_point, value] pair: %v", pair)
		}
		codePoint, want := rune(pair[0]), pair[1]
		if got := decimalDigitValue(codePoint); got != want {
			if mismatches < 15 {
				t.Errorf("decimalDigitValue(U+%04X) = %d, interpreter says %d",
					codePoint, got, want)
			}
			mismatches++
		}
	}
	if mismatches > 15 {
		t.Errorf("... and %d further mismatches", mismatches-15)
	}
}

// TestDecimalSetMatchesTheInterpreterAndNotGoTables is the version-skew guard.
//
// Go's unicode tables and CPython's are versioned INDEPENDENTLY, and on this
// toolchain they ALREADY DISAGREE: Go 1.27 ships Unicode 17, the interpreter
// reports 16, and U+11DE0-U+11DE9 are Nd to Go and unassigned to Python. Using
// unicode.IsDigit as the implementation would accept ten code points Python
// refuses -- today, not hypothetically.
//
// That is why decimalDigitValue is backed by pythonDecimalRuns, generated from
// the deployed interpreter. This test asserts the property that matters -- the
// port's accept set is PYTHON's -- and specifically catches a regression to
// Go's tables by checking the divergent code points are refused.
//
// It compares derived SETS, never version strings: a Go or CPython bump that
// does not move Nd leaves this green, and one that does move it fails here
// rather than silently changing which configuration values parse.
func TestDecimalSetMatchesTheInterpreterAndNotGoTables(t *testing.T) {
	golden := loadDecimalDigitsGolden(t)

	pythonDecimal := make(map[rune]bool, len(golden.DecimalDigits))
	for _, pair := range golden.DecimalDigits {
		pythonDecimal[rune(pair[0])] = true
	}

	// Exhaustive: the port's accept set must equal the interpreter's, over the
	// whole code space rather than a sample.
	var portOnly, pythonOnly []rune
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		switch accepted, expected := decimalDigitValue(codePoint) >= 0, pythonDecimal[codePoint]; {
		case accepted && !expected:
			portOnly = append(portOnly, codePoint)
		case !accepted && expected:
			pythonOnly = append(pythonOnly, codePoint)
		}
	}
	if len(portOnly) > 0 || len(pythonOnly) > 0 {
		t.Errorf("the port's decimal set differs from the interpreter's\n"+
			"  accepted by the port, refused by Python: %d %v\n"+
			"  accepted by Python, refused by the port: %d %v\n"+
			"regenerate pythonDecimalRuns with "+
			"tests/fixtures/generate_python_decimal_digits_golden.py",
			len(portOnly), truncateRunes(portOnly),
			len(pythonOnly), truncateRunes(pythonOnly))
	}

	// The regression this exists to catch: swapping the generated table back
	// for unicode.IsDigit. Any code point where the two toolchains disagree
	// must be refused by the port, because Python refuses it.
	var skew []rune
	for codePoint := rune(0); codePoint <= 0x10FFFF; codePoint++ {
		if unicode.IsDigit(codePoint) && !pythonDecimal[codePoint] {
			skew = append(skew, codePoint)
			if decimalDigitValue(codePoint) >= 0 {
				t.Errorf("U+%04X is Nd to Go (Unicode %s) but not to the "+
					"interpreter (Unicode %s), and the port ACCEPTS it -- "+
					"decimalDigitValue is reading Go's tables again instead of "+
					"pythonDecimalRuns",
					codePoint, unicode.Version, golden.UnidataVersion)
			}
		}
	}
	if len(skew) == 0 && unicode.Version != golden.UnidataVersion {
		t.Logf("Go is Unicode %s and the interpreter is Unicode %s, but their Nd "+
			"sets agree; the skew guard is inert until they diverge",
			unicode.Version, golden.UnidataVersion)
	} else if len(skew) > 0 {
		t.Logf("measured skew: %d code points are Nd to Go (Unicode %s) and not "+
			"to the interpreter (Unicode %s); all correctly refused",
			len(skew), unicode.Version, golden.UnidataVersion)
	}

	// isdigit() but NOT isdecimal(): int() REJECTS these. One letter apart in
	// Python, 128 code points apart in behaviour.
	for _, codePoint := range golden.DigitButNotDecimal {
		if got := decimalDigitValue(rune(codePoint)); got >= 0 {
			t.Errorf("decimalDigitValue(U+%04X) = %d, but int() REJECTS it: it is "+
				"isdigit() and not isdecimal()", codePoint, got)
		}
	}
	if len(golden.DigitButNotDecimal) == 0 {
		t.Error("the golden lists no isdigit-but-not-isdecimal code points; that " +
			"distinction is untested")
	}
}

func truncateRunes(values []rune) []rune {
	if len(values) > 8 {
		return values[:8]
	}
	return values
}

// TestDigitLimitCountsCharactersNotBytes pins the unit of the digit limit.
//
// CPython counts CHARACTERS. 4300 full-width digits is 4300 characters and
// 12900 BYTES, so a port measuring len(builder) in bytes refuses a value Python
// accepts -- and the refusal is invisible, because it looks like the ordinary
// fallback to the default.
func TestDigitLimitCountsCharactersNotBytes(t *testing.T) {
	// Full-width digit five, U+FF15: three bytes per character.
	const fullWidthFive = "５"
	atLimit := strings.Repeat(fullWidthFive, DefaultIntMaxStrDigits)
	overLimit := strings.Repeat(fullWidthFive, DefaultIntMaxStrDigits+1)

	if runes, bytes := len([]rune(atLimit)), len(atLimit); runes != DefaultIntMaxStrDigits ||
		bytes != DefaultIntMaxStrDigits*3 {
		t.Fatalf("sanity: %d runes / %d bytes, want %d / %d",
			runes, bytes, DefaultIntMaxStrDigits, DefaultIntMaxStrDigits*3)
	}

	// At the limit Python parses (to a huge value), so Go saturates.
	if _, ok := parsePythonInt(atLimit); !ok {
		t.Errorf("%d full-width digits (%d bytes) must be ACCEPTED: the limit "+
			"counts characters, and a byte-based count would refuse this",
			DefaultIntMaxStrDigits, len(atLimit))
	}
	// One past it Python raises, so Go refuses.
	if _, ok := parsePythonInt(overLimit); ok {
		t.Errorf("%d full-width digits must be REFUSED", DefaultIntMaxStrDigits+1)
	}
}
