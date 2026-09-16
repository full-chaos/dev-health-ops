package pythonparity

import (
	"math"
	"testing"
)

// TestFloatJSONMatchesLivePythonJSONDumps pins FloatJSON against real
// `python3 -c "import json; json.dumps(<value>)"` output (values and their
// exact printed strings measured against the interpreter, not hand-derived).
// These cases pin the notation-threshold boundaries (exponent -4 and 16) on
// both sides, including the exact value where CPython's shortest-digit-count
// switch and its absolute-exponent switch disagree: a single-significant-digit
// value like 1_000_000.0 stays fixed ("1000000.0") in CPython's rule while a
// digit-count-relative rule like Go's 'g' verb would emit scientific notation
// ("1e+06").
func TestFloatJSONMatchesLivePythonJSONDumps(t *testing.T) {
	cases := []struct {
		value float64
		want  string
	}{
		{0.0, "0.0"},
		{1.0, "1.0"},
		{-1.0, "-1.0"},
		{0.15, "0.15"},
		{0.7333, "0.7333"},
		{942.0, "942.0"},
		{60.0, "60.0"},
		{1000000.0, "1000000.0"},
		{-1000000.0, "-1000000.0"},
		{999999.0, "999999.0"},
		{9999999999999998.0, "9999999999999998.0"}, // exponent 15: still fixed
		{1e16, "1e+16"},              // exponent 16: scientific
		{1e15, "1000000000000000.0"}, // exponent 15: still fixed
		{0.0001, "0.0001"},           // exponent -4: still fixed
		{0.00001, "1e-05"},           // exponent -5: scientific
		{100000000000000.0, "100000000000000.0"},
		{123456789012345.0, "123456789012345.0"},
		{0.6, "0.6"},
		{0.05, "0.05"},
		{2.5, "2.5"},
		{3600.0, "3600.0"},
	}
	for _, tc := range cases {
		if got := FloatJSON(tc.value); got != tc.want {
			t.Errorf("FloatJSON(%v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

// TestFloatJSONNegativeZero pins Python's json.dumps(-0.0) == "-0.0"
// separately: -0.0 == 0.0 under Go's ==, so it needs math.Signbit, not a
// value comparison, to distinguish from the case above.
func TestFloatJSONNegativeZero(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	if got := FloatJSON(negativeZero); got != "-0.0" {
		t.Errorf("FloatJSON(-0.0) = %q, want \"-0.0\"", got)
	}
}

// TestFloatJSONNonFiniteValuesReturnValidJSONNull proves FloatJSON returns
// the JSON literal `null` for NaN/+-Inf rather than Python's own non-spec
// `allow_nan=True` tokens ("NaN"/"Infinity"/"-Infinity"), which are not valid
// JSON -- deliberately NOT mirrored here, since every caller wants a
// syntactically valid document.
func TestFloatJSONNonFiniteValuesReturnValidJSONNull(t *testing.T) {
	cases := []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, value := range cases {
		got := FloatJSON(value) // must not panic
		if got != "null" {
			t.Errorf("FloatJSON(%v) = %q, want \"null\" (valid JSON, never a NaN/Infinity token)", value, got)
		}
	}
}
