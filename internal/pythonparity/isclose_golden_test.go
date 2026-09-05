package pythonparity

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

type iscloseCase struct {
	A            string `json:"a"`
	B            string `json:"b"`
	RelTol       string `json:"rel_tol"`
	AbsTol       string `json:"abs_tol"`
	Close        bool   `json:"close"`
	NaiveAbsOnly *bool  `json:"naive_abs_only"`
}

func loadIsCloseGolden(t *testing.T) []iscloseCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(
		"../../tests/fixtures/python_isclose_python_golden.json",
	))
	if err != nil {
		t.Fatalf("read isclose golden: %v (regenerate with: PYTHONPATH=src python "+
			"tests/fixtures/generate_python_isclose_golden.py)", err)
	}
	var golden struct {
		Cases []iscloseCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse isclose golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden.Cases
}

// decodeIsCloseFloat parses a CPython repr. strconv.ParseFloat accepts "inf",
// "-inf" and "nan" case-insensitively, which is exactly what repr emits.
func decodeIsCloseFloat(t *testing.T, text string) float64 {
	t.Helper()
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		t.Fatalf("parse %q as float64: %v", text, err)
	}
	return value
}

// TestIsCloseMatchesCPython is the differential oracle for IsClose: every case
// in the frozen corpus, compared against CPython's own answer.
func TestIsCloseMatchesCPython(t *testing.T) {
	for _, testCase := range loadIsCloseGolden(t) {
		a := decodeIsCloseFloat(t, testCase.A)
		b := decodeIsCloseFloat(t, testCase.B)
		relTol := decodeIsCloseFloat(t, testCase.RelTol)
		absTol := decodeIsCloseFloat(t, testCase.AbsTol)

		if got := IsClose(a, b, relTol, absTol); got != testCase.Close {
			t.Errorf(
				"IsClose(%s, %s, rel_tol=%s, abs_tol=%s) = %v, CPython says %v",
				testCase.A, testCase.B, testCase.RelTol, testCase.AbsTol, got, testCase.Close,
			)
		}
	}
}

// TestTheCorpusActuallyDiscriminatesAgainstTheNaiveForm is the guard on the
// GUARD. The whole point of this helper is that `math.Abs(a-b) <= absTol` is
// wrong, so the corpus has to contain cases where the two forms disagree --
// otherwise TestIsCloseMatchesCPython above would pass just as happily against
// the naive implementation and prove nothing.
//
// It also pins the failure mode that hid this class: comparisons against ZERO
// agree with the naive form, so a corpus of only those is worthless here.
func TestTheCorpusActuallyDiscriminatesAgainstTheNaiveForm(t *testing.T) {
	cases := loadIsCloseGolden(t)

	discriminating := 0
	for _, testCase := range cases {
		if testCase.NaiveAbsOnly == nil {
			continue // NaN/Inf case; the naive form is not even defined there
		}
		a := decodeIsCloseFloat(t, testCase.A)
		b := decodeIsCloseFloat(t, testCase.B)
		absTol := decodeIsCloseFloat(t, testCase.AbsTol)

		naive := math.Abs(a-b) <= absTol
		if naive != *testCase.NaiveAbsOnly {
			t.Errorf(
				"corpus disagrees with Go on the naive form for a=%s b=%s abs_tol=%s: got %v, fixture says %v",
				testCase.A, testCase.B, testCase.AbsTol, naive, *testCase.NaiveAbsOnly,
			)
		}
		if testCase.Close != naive {
			discriminating++
		}
	}

	if discriminating == 0 {
		t.Fatal(
			"the corpus contains ZERO cases where CPython's isclose disagrees with " +
				"`abs(a-b) <= abs_tol` -- it therefore cannot detect the very bug this " +
				"helper exists to prevent. Add large-magnitude operand pairs that differ " +
				"by more than abs_tol but less than rel_tol*max(|a|,|b|).",
		)
	}
	t.Logf("%d of %d cases discriminate against the naive absolute-tolerance form", discriminating, len(cases))
}

// TestIsCloseEdgeCases states CPython's short-circuits directly, so a failure
// is diagnosable rather than appearing as one row among 1805.
func TestIsCloseEdgeCases(t *testing.T) {
	infinity := math.Inf(1)
	negInfinity := math.Inf(-1)
	notANumber := math.NaN()

	// Identical values are close even at infinity: CPython tests `a == b`
	// BEFORE subtracting, and inf-inf would be NaN.
	if !IsClose(infinity, infinity, DefaultRelTol, 0.0) {
		t.Error("IsClose(inf, inf) = false, CPython says True (it short-circuits on equality)")
	}
	if IsClose(infinity, negInfinity, DefaultRelTol, 0.0) {
		t.Error("IsClose(inf, -inf) = true, CPython says False")
	}
	// NaN is never close to anything, including itself.
	if IsClose(notANumber, notANumber, DefaultRelTol, 1.0) {
		t.Error("IsClose(nan, nan) = true, CPython says False")
	}
	// A huge abs_tol does not rescue an infinity mismatch.
	if IsClose(infinity, 1.0, DefaultRelTol, math.MaxFloat64) {
		t.Error("IsClose(inf, 1.0, abs_tol=max) = true, CPython says False")
	}
	// The documented headline case, stated inline.
	if !IsCloseAbs(1000.0, 1000.0000005, 1e-9) {
		t.Error("IsCloseAbs(1000.0, 1000.0000005, 1e-9) = false, CPython says True -- rel_tol is not being applied")
	}
	if math.Abs(1000.0-1000.0000005) <= 1e-9 {
		t.Fatal("precondition failed: the naive form now agrees, so this case no longer discriminates")
	}
}
