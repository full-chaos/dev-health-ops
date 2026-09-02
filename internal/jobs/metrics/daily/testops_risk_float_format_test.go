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

// TestPythonFloatJSONNonFiniteValuesDoNotPanic is the red-on-baseline proof
// for codex round 3 (P2, ARGUED then confirmed by source read): before this
// guard existed, strconv.FormatFloat(value, 'e', -1, 64) on NaN/+-Inf
// returns "NaN"/"+Inf"/"-Inf" -- none contain the byte 'e' -- so
// strings.IndexByte(scientific, 'e') returned -1 and `scientific[:eIndex]`
// PANICKED (slice bounds out of range [:-1]). This is a real input, not a
// hypothetical one: coverage_snapshots.line_coverage_pct is an
// unconstrained Nullable(Float64) with no finite-value guard on the Python
// writer side. Pinned against real `python3 -c "import json;
// json.dumps(float('nan'))"` output -- Python's json module (default
// allow_nan=True) emits the literal tokens "NaN"/"Infinity"/"-Infinity",
// not valid JSON per spec but exactly what the Python authority writes.
func TestPythonFloatJSONNonFiniteValuesDoNotPanic(t *testing.T) {
	cases := []struct {
		value float64
		want  string
	}{
		{math.NaN(), "NaN"},
		{math.Inf(1), "Infinity"},
		{math.Inf(-1), "-Infinity"},
	}
	for _, tc := range cases {
		got := pythonFloatJSON(tc.value) // must not panic
		if got != tc.want {
			t.Errorf("pythonFloatJSON(%v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

// TestClampUnitMatchesPythonClampSemantics pins clampUnit against Python's
// `_clamp(value, lo=0.0, hi=1.0) -> max(lo, min(hi, value))`
// (compute_testops_risk.py:20-21), including its NaN behavior -- codex
// round 4 (P2, EXECUTED via a temporary go test -overlay calling the real
// production computeReleaseConfidence): a naive `if value < 0 {0} else if
// value > 1 {1} else {value}` implementation falls through to `value`
// unchanged on NaN (both Go comparisons are false for NaN, same as
// Python's), but CPython's min()/max() start from a specific FIRST
// argument and only replace it on a strict comparison, so
// `min(1.0, nan)` keeps 1.0 (nan < 1.0 is false) and `max(0.0, 1.0)` then
// keeps 1.0 (1.0 > 0.0 is true) -- NaN always resolves to hi=1.0, not to
// itself. Verified against a live `python3` interpreter:
// `max(0.0, min(1.0, float('nan'))) == 1.0`. Reachable in production:
// coverage_snapshots.line_coverage_pct is an unconstrained Nullable(Float64)
// that flows into coveragePct/100.0 here.
func TestClampUnitMatchesPythonClampSemantics(t *testing.T) {
	cases := []struct {
		name  string
		value float64
		want  float64
	}{
		{"in range unchanged", 0.5, 0.5},
		{"below lo clamps to 0", -0.3, 0.0},
		{"above hi clamps to 1", 1.5, 1.0},
		{"exactly lo", 0.0, 0.0},
		{"exactly hi", 1.0, 1.0},
		{"NaN resolves to hi, matching Python", math.NaN(), 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampUnit(tc.value)
			if got != tc.want {
				t.Errorf("clampUnit(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
