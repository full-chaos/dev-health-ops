package textrefs

import (
	"encoding/json"
	"math/big"
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
	output := []byte(textrefsProgram(t, "number_allrunes", "digit values"))
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

// TestPythonAtoiMatchesLivePythonAroundTheInt64Boundary pins the exact int64
// boundary of pythonAtoi against what the producer's int() returns for the same
// runs. Python's int() is arbitrary precision, so above int64 Go has no value to
// return and refuses (the declared divergence in the package doc); at or below
// int64 the two must agree to the digit. A guard that is one step too eager
// (accepting ...808, which wraps negative) or one step too strict (refusing
// ...807, the value the boundary comment cites) fails here.
//
// The values were rendered by int() once, on the last build that carried the
// Python sources, and are frozen in testdata/golden/atoi_boundary.json.
func TestPythonAtoiMatchesLivePythonAroundTheInt64Boundary(t *testing.T) {
	output := []byte(textrefsProgram(t, "atoi_boundary", "atoi boundary"))
	var want []struct {
		Run   string `json:"run"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode the recorded int() values: %v", err)
	}
	if len(want) < 19 {
		t.Fatalf("the recording holds %d runs; the comparison would pass on a truncated corpus", len(want))
	}
	const maxInt64 = "9223372036854775807"
	for _, entry := range want {
		got, ok := pythonAtoi(entry.Run)
		pythonValue, _ := new(big.Int).SetString(entry.Value, 10)
		fits := pythonValue.IsInt64()
		if fits != ok {
			t.Errorf("%q: Python int() gives %s (fits int64: %v) but pythonAtoi ok=%v (the boundary is %s)", entry.Run, entry.Value, fits, ok, maxInt64)
			continue
		}
		if ok && int64(got) != pythonValue.Int64() {
			t.Errorf("%q: Python int() gives %s, pythonAtoi gives %d", entry.Run, entry.Value, got)
		}
	}
}
