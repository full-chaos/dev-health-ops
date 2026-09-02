package remaining

import (
	"encoding/json"
	"math"
	"os"
	"strconv"
	"testing"
)

type signalsGolden struct {
	Marker                string `json:"_marker"`
	GeneratingInterpreter struct {
		PythonVersion  string `json:"python_version"`
		Implementation string `json:"implementation"`
		Machine        string `json:"machine"`
		FloatReprStyle string `json:"float_repr_style"`
	} `json:"generating_interpreter"`
	Cases []struct {
		ValuesHex  []string `json:"values_hex"`
		GiniHex    *string  `json:"gini_hex"`
		GiniIsNone bool     `json:"gini_is_none"`
		SlopeHex   string   `json:"slope_hex"`
	} `json:"cases"`
}

func parseHexFloat(t *testing.T, text string) float64 {
	t.Helper()
	switch text {
	case "nan":
		return math.NaN()
	case "inf":
		return math.Inf(1)
	case "-inf":
		return math.Inf(-1)
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return value
}

func loadSignalsGolden(t *testing.T) signalsGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/recommendations_signals_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden signalsGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if golden.Marker != "recommendations-signals-golden" {
		t.Fatalf("golden marker is %q; wrong file", golden.Marker)
	}
	// An empty or truncated corpus would pass every assertion below by having
	// nothing to assert, which reads as coverage.
	if len(golden.Cases) == 0 {
		t.Fatal("golden has no cases; regenerate it")
	}
	return golden
}

// TestGiniMatchesLivePython and TestLinearSlopeMatchesLivePython compare
// BITWISE, not with ==. Both feed categorical thresholds (0.6 for gini, 0.1 for
// both slope constants), so a last-bit difference is a stored recommendation
// row that either exists or does not; and == would treat +0.0 and -0.0 as
// equal, which is exactly the distinction the -0.0 cases in the corpus exist
// to pin.
func TestGiniMatchesLivePython(t *testing.T) {
	golden := loadSignalsGolden(t)

	noneCases := 0
	for _, testCase := range golden.Cases {
		values := make([]float64, len(testCase.ValuesHex))
		for index, text := range testCase.ValuesHex {
			values[index] = parseHexFloat(t, text)
		}

		got, ok := Gini(values)
		if testCase.GiniIsNone {
			if ok {
				t.Errorf("Gini(%v) = %v, present; python returned None", testCase.ValuesHex, got)
			}
			noneCases++
			continue
		}
		if !ok {
			t.Errorf("Gini(%v) absent; python returned %s", testCase.ValuesHex, *testCase.GiniHex)
			continue
		}
		want := parseHexFloat(t, *testCase.GiniHex)
		// NaN is compared as NaN, not bitwise. The corpus transports it as the
		// string "nan" (float.hex renders it that way), so the PAYLOAD is not
		// carried and cannot be part of the contract -- and it genuinely
		// differs: Go's math.NaN() is 0x7ff8000000000001 while a NaN produced
		// by arithmetic here is 0x7ff8000000000000. Comparing payloads would
		// fail on agreement.
		//
		// Every other value stays bitwise, because +0.0 and -0.0 must not
		// compare equal. LinearSlope's test already did this; Gini's did not,
		// and the two halves of one file disagreeing is what the first run of
		// this test found.
		if math.IsNaN(want) {
			if !math.IsNaN(got) {
				t.Errorf("Gini(%v) = %v; python produced nan", testCase.ValuesHex, got)
			}
			continue
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Errorf("Gini(%v) = %v (bits %#016x); python = %v (bits %#016x)",
				testCase.ValuesHex, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}

	// The None branch is a distinct contract (fewer than two positive entries),
	// not an edge case, so assert the corpus still exercises it.
	if noneCases == 0 {
		t.Fatal("no corpus case where python's _gini returns None; that branch is untested")
	}
	t.Logf("gini cases: %d, of which None: %d", len(golden.Cases), noneCases)
}

func TestLinearSlopeMatchesLivePython(t *testing.T) {
	golden := loadSignalsGolden(t)

	for _, testCase := range golden.Cases {
		values := make([]float64, len(testCase.ValuesHex))
		for index, text := range testCase.ValuesHex {
			values[index] = parseHexFloat(t, text)
		}

		got := LinearSlope(values)
		want := parseHexFloat(t, testCase.SlopeHex)

		if math.IsNaN(want) {
			if !math.IsNaN(got) {
				t.Errorf("LinearSlope(%v) = %v; python produced nan", testCase.ValuesHex, got)
			}
			continue
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Errorf("LinearSlope(%v) = %v (bits %#016x); python = %v (bits %#016x)",
				testCase.ValuesHex, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}
}

// TestNaiveSummationDisagreesWithTheReference is a discriminating test, not a
// redundant one: it pins the REASON pythonparity.Sum is used here.
//
// If a future change replaced Sum with a `+=` loop, the golden above would
// catch it -- but only on the inputs that happen to differ. This states the
// property directly, on the measured series from the receipt, so the failure
// names the cause instead of looking like an unrelated corpus mismatch.
func TestNaiveSummationDisagreesWithTheReference(t *testing.T) {
	// Measured: CPython gives exactly 0.1 here and the recommendation fires;
	// naive accumulation gives 0.09999999999999999 and it does not.
	values := []float64{0.1, 0.0, 1.0, 0.8, 0.2}

	compensated := LinearSlope(values)
	if compensated != 0.1 {
		t.Fatalf("LinearSlope(%v) = %v; the reference gives exactly 0.1", values, compensated)
	}

	naive := naiveLinearSlope(values)
	if naive == compensated {
		t.Errorf("naive accumulation now agrees with the compensated one on %v; "+
			"the documented divergence this function's Sum usage exists for is gone, "+
			"so the rationale needs rewriting", values)
	}
	if naive >= 0.1 {
		t.Errorf("naive slope = %v, which still fires the 0.1 threshold; the "+
			"categorical consequence this test pins has changed", naive)
	}
}

// naiveLinearSlope is the wrong implementation, kept only so the test above can
// show it is wrong. It must never be called by production code.
func naiveLinearSlope(values []float64) float64 {
	count := len(values)
	if count < 2 {
		return 0.0
	}
	meanIndex := float64(count-1) / 2.0
	total := 0.0
	for _, value := range values {
		total += value
	}
	meanValue := total / float64(count)
	numerator, denominator := 0.0, 0.0
	for index, value := range values {
		offset := float64(index) - meanIndex
		numerator += offset * (value - meanValue)
		denominator += offset * offset
	}
	if denominator == 0 {
		return 0.0
	}
	return numerator / denominator
}
