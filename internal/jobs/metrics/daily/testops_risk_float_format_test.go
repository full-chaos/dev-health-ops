package daily

import (
	"math"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/finite"
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

// TestPythonFloatJSONNonFiniteValuesReturnValidJSONNull is the R73 (CHAOS-4806)
// replacement for this test's old expectation. Before this fix, the assertion
// HERE was `want: "NaN"/"Infinity"/"-Infinity"` -- i.e. this exact test used to
// PIN the defect it now guards against (Trap #116: a test asserting the
// defective behaviour looks like coverage while holding the bug in place).
// factorsJSON's caller now routes every float through finite.JSONLiteral
// before pythonFloatJSON ever sees it, so in production a non-finite value
// never reaches this function at all; the case below exercises
// pythonFloatJSON's own defensive fallback directly, proving it still
// returns valid JSON (never panics, never Python's non-spec allow_nan=True
// tokens) even on direct misuse. See TestFactorsJSONNeverEmitsNonFiniteTokens
// below for the actual write-boundary proof (through finite.JSONLiteral).
func TestPythonFloatJSONNonFiniteValuesReturnValidJSONNull(t *testing.T) {
	cases := []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, value := range cases {
		got := pythonFloatJSON(value) // must not panic
		if got != "null" {
			t.Errorf("pythonFloatJSON(%v) = %q, want \"null\" (valid JSON, never a NaN/Infinity token)", value, got)
		}
	}
}

// TestFactorsJSONNeverEmitsNonFiniteTokens is the wire-level red-first proof
// for CHAOS-4806 / ruling R73 at the actual write boundary (factorsJSON,
// this file's real JSON serializer for metric factors): scans the RENDERED
// payload for the forbidden tokens rather than hand-listing call sites, so a
// future field added to either factors_json producer is covered
// automatically. Before finite.JSONLiteral was wired into factorsJSON, a
// NaN input here rendered the literal 3-byte token "NaN" (and Infinity/
// -Infinity) directly into the row's factors_json column -- not valid JSON,
// and exactly the "NaN/Inf on the wire" defect CHAOS-4806 exists to close.
func TestFactorsJSONNeverEmitsNonFiniteTokens(t *testing.T) {
	forbidden := []string{"NaN", "Infinity"}
	got := factorsJSON("test_family", []factorsJSONField{
		ff("finite_value", 1.5),
		ff("nan_value", math.NaN()),
		ff("positive_inf_value", math.Inf(1)),
		ff("negative_inf_value", math.Inf(-1)),
		fi("int_value", 7),
	})
	for _, bad := range forbidden {
		if strings.Contains(got, bad) {
			t.Fatalf("factorsJSON output contains forbidden wire token %q: %s", bad, got)
		}
	}
	want := `{"finite_value": 1.5, "nan_value": null, "positive_inf_value": null, "negative_inf_value": null, "int_value": 7}`
	if got != want {
		t.Fatalf("factorsJSON(...) = %s, want %s", got, want)
	}
}

// TestFactorsJSONNonFiniteFieldsCountedByFamilyAndField proves the R73
// telemetry requirement: a boundary trip records a bounded reason code
// against the calling family and the specific field key, not just a log
// line -- so CHAOS-4806 defects are visible at the counter, per field.
func TestFactorsJSONNonFiniteFieldsCountedByFamilyAndField(t *testing.T) {
	family := "test_family_counted"
	beforeNaN := finite.Count(family, "nan_field", finite.ReasonNaN)
	beforeInf := finite.Count(family, "inf_field", finite.ReasonPositiveInf)

	factorsJSON(family, []factorsJSONField{
		ff("nan_field", math.NaN()),
		ff("inf_field", math.Inf(1)),
	})

	if after := finite.Count(family, "nan_field", finite.ReasonNaN); after != beforeNaN+1 {
		t.Errorf("finite.Count(nan_field, nan) = %d, want %d", after, beforeNaN+1)
	}
	if after := finite.Count(family, "inf_field", finite.ReasonPositiveInf); after != beforeInf+1 {
		t.Errorf("finite.Count(inf_field, +inf) = %d, want %d", after, beforeInf+1)
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
