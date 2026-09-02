package daily

import (
	"math"
	"testing"
)

// TestPythonFloatJSONMatchesLivePythonJSONDumps pins pythonFloatJSON against
// real `python3 -c "import json; json.dumps(<value>)"` output (values and
// their exact printed strings captured this session, not hand-derived).
// Codex round 2 (P2, EXECUTED) caught the prior strconv.FormatFloat('g', -1,
// 64) implementation printing "1e+06" for 1_000_000.0, where Python's
// json.dumps prints "1000000.0" -- these cases pin that fix and the
// notation-threshold boundaries (exponent -4 and 16) on both sides.
func TestPythonFloatJSONMatchesLivePythonJSONDumps(t *testing.T) {
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
		{1000000.0, "1000000.0"}, // codex's exact repro
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
		if got := pythonFloatJSON(tc.value); got != tc.want {
			t.Errorf("pythonFloatJSON(%v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

// TestPythonFloatJSONNegativeZero pins Python's json.dumps(-0.0) == "-0.0"
// separately: -0.0 == 0.0 in Go's == operator, so it needs math.Signbit,
// not a value comparison, to distinguish from the case above.
func TestPythonFloatJSONNegativeZero(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	if got := pythonFloatJSON(negativeZero); got != "-0.0" {
		t.Errorf("pythonFloatJSON(-0.0) = %q, want \"-0.0\"", got)
	}
}
