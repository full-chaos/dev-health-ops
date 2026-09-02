package pythonparity

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

type sumCase struct {
	Values []json.RawMessage `json:"values"`
	Sum    string            `json:"sum"`
}

func loadSumGolden(t *testing.T) []sumCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(
		"../../tests/fixtures/python_sum_python_golden.json",
	))
	if err != nil {
		t.Fatalf("read sum golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_python_sum_golden.py)", err)
	}
	var golden struct {
		Cases []sumCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse sum golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden contains no cases")
	}
	return golden.Cases
}

func decodeSumFloat(t *testing.T, raw json.RawMessage) float64 {
	t.Helper()
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return parsePythonFloatLiteral(t, text)
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return value
}

func parsePythonFloatLiteral(t *testing.T, text string) float64 {
	t.Helper()
	switch text {
	case "inf":
		return math.Inf(1)
	case "-inf":
		return math.Inf(-1)
	case "nan":
		return math.NaN()
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		t.Fatalf("parse python float %q: %v", text, err)
	}
	return value
}

func sameFloatBits(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

// TestSumMatchesCPython drives every corpus case, and drives the NAIVE
// accumulation alongside so the size of the divergence is recorded rather than
// merely averted.
//
// CPython's sum() has used Neumaier compensated summation for floats since 3.12
// (gh-100425). A `total += x` loop is not equivalent, and the corpus reports
// how often -- roughly two in five cases here.
func TestSumMatchesCPython(t *testing.T) {
	cases := loadSumGolden(t)

	var mismatches, naiveMismatches, longest int
	for _, testCase := range cases {
		values := make([]float64, len(testCase.Values))
		for index, raw := range testCase.Values {
			values[index] = decodeSumFloat(t, raw)
		}
		if len(values) > longest {
			longest = len(values)
		}
		want := parsePythonFloatLiteral(t, testCase.Sum)

		if got := Sum(values); !sameFloatBits(got, want) {
			if mismatches < 10 {
				t.Errorf("Sum(%d values) = %v, CPython = %v", len(values), got, want)
			}
			mismatches++
		}

		naive := 0.0
		for _, value := range values {
			naive += value
		}
		if !sameFloatBits(naive, want) {
			naiveMismatches++
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d further mismatches", mismatches-10)
	}

	// Guard the corpus on the axis that makes this defect invisible. With fewer
	// than three summands the compensation is always zero, so a short-list
	// corpus cannot tell compensated from naive however many VALUES it varies.
	if longest < 20 {
		t.Errorf("longest case has %d summands; the discriminating axis is LENGTH, "+
			"and short lists cannot distinguish compensated from naive summation",
			longest)
	}
	if naiveMismatches == 0 {
		t.Error("naive accumulation agreed with CPython on every case; the corpus " +
			"no longer exercises the compensation and would pass a wrong port")
	}
	t.Logf("naive accumulation differs from CPython on %d of %d cases",
		naiveMismatches, len(cases))
}

// TestSumCompensationIsInvisibleBelowThreeSummands states WHY the axis is
// length, so a future corpus trim does not quietly remove the only cases that
// discriminate.
func TestSumCompensationIsInvisibleBelowThreeSummands(t *testing.T) {
	for _, values := range [][]float64{
		{},
		{0.1},
		{0.1, 0.2},
		{1e16, 1.0},
	} {
		naive := 0.0
		for _, value := range values {
			naive += value
		}
		if got := Sum(values); !sameFloatBits(got, naive) {
			t.Errorf("with %d summands Sum and naive should agree, got %v vs %v",
				len(values), got, naive)
		}
	}

	// Three is where they part company.
	three := []float64{0.1, 0.2, 0.3}
	naive := 0.0
	for _, value := range three {
		naive += value
	}
	if Sum(three) == naive {
		t.Error("Sum and naive agree on [0.1 0.2 0.3]; the compensation is not " +
			"being applied")
	}
	if got := Sum(three); got != 0.6 {
		t.Errorf("Sum([0.1 0.2 0.3]) = %v, CPython = 0.6", got)
	}
}
