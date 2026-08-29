package graphqljson

import (
	"bytes"
	"encoding/json"
	"testing"
)

// The three forms enumerated from the actual Python producer
// (ops/src/dev_health_ops/api/graphql/resolvers/analytics.py:968-972 and
// models/outputs.py:119-134) for THIS ticket's two fields:
//
//  1. `null` -- AnalyticsResult.evidenceQualityDistribution when
//     evidence_quality_stats is None (analytics.py:968-972: `... if
//     evidence_quality_stats is not None else None`).
//  2. `{}` -- EvidenceQualityStats.bandCounts's pre-computed default
//     (outputs.py:123 `default_factory=dict`), returned whenever
//     `_resolve_evidence_quality_stats` short-circuits to an empty
//     `EvidenceQualityStats()` (analytics.py:252-253: `if not row: return
//     EvidenceQualityStats()`).
//  3. The populated 5-key int-valued object -- band_counts built at
//     analytics.py:255-261, always exactly {"high", "moderate", "low",
//     "very_low", "unknown"} -> int.
//
// These are the ONLY forms this ticket's fields ever take; the JSON
// scalar's OTHER consumers (SavedReport etc.) are unresolved stubs today
// (see json.go's package doc), so this file does not attempt to pin their
// shapes.

func TestMarshalGQL_Null(t *testing.T) {
	cases := []struct {
		name string
		j    JSON
	}{
		{"zero value", JSON(nil)},
		{"explicit Null", Null},
		{"empty non-nil slice", JSON([]byte{})},
		{"literal null bytes", JSON([]byte("null"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tc.j.MarshalGQL(&buf)
			if got := buf.String(); got != "null" {
				t.Fatalf("MarshalGQL() = %q, want %q", got, "null")
			}
			if !tc.j.IsNull() {
				t.Fatalf("IsNull() = false, want true")
			}
		})
	}
}

func TestFromValue_Null(t *testing.T) {
	j, err := FromValue(nil)
	if err != nil {
		t.Fatalf("FromValue(nil) error = %v", err)
	}
	var buf bytes.Buffer
	j.MarshalGQL(&buf)
	if got := buf.String(); got != "null" {
		t.Fatalf("MarshalGQL() = %q, want %q", got, "null")
	}
}

func TestFromValue_EmptyObject(t *testing.T) {
	// EvidenceQualityStats.bandCounts's pre-stats default: dict() -> {}.
	j, err := FromValue(map[string]int{})
	if err != nil {
		t.Fatalf("FromValue(map[string]int{}) error = %v", err)
	}
	if j.IsNull() {
		t.Fatalf("IsNull() = true for {} -- an empty JSON OBJECT is not JSON null")
	}
	var buf bytes.Buffer
	j.MarshalGQL(&buf)
	if got := buf.String(); got != "{}" {
		t.Fatalf("MarshalGQL() = %q, want %q", got, "{}")
	}
}

func TestFromValue_PopulatedBandCounts(t *testing.T) {
	// The real shape: exactly 5 fixed keys -> int, analytics.py:255-261.
	bandCounts := map[string]int{
		"high":     3,
		"moderate": 5,
		"low":      2,
		"very_low": 1,
		"unknown":  0,
	}
	j, err := FromValue(bandCounts)
	if err != nil {
		t.Fatalf("FromValue(bandCounts) error = %v", err)
	}
	var buf bytes.Buffer
	j.MarshalGQL(&buf)

	got := buf.Bytes()
	var roundTripped map[string]int
	if err := json.Unmarshal(got, &roundTripped); err != nil {
		t.Fatalf("marshaled JSON does not decode: %v (bytes: %s)", err, got)
	}
	if len(roundTripped) != 5 {
		t.Fatalf("round-tripped map has %d keys, want 5: %v", len(roundTripped), roundTripped)
	}
	for k, want := range bandCounts {
		if got := roundTripped[k]; got != want {
			t.Fatalf("key %q = %d, want %d", k, got, want)
		}
	}
}

// UnmarshalGQL exercises the reverse direction: gqlgen hands this type a
// decoded Go value (as if a client sent a JSON literal/variable), and it
// must round-trip every ECMA-404 shape -- not just objects, unlike
// graphql.Map, which this scalar replaces specifically because it could
// not do this (see json.go's package doc).
func TestUnmarshalGQL_AllECMA404Shapes(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, "null"},
		{"object", map[string]any{"high": float64(3)}, `{"high":3}`},
		{"empty object", map[string]any{}, "{}"},
		{"array", []any{"a", "b"}, `["a","b"]`},
		{"string", "hello", `"hello"`},
		{"number", float64(42), "42"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var j JSON
			if err := j.UnmarshalGQL(tc.in); err != nil {
				t.Fatalf("UnmarshalGQL(%#v) error = %v", tc.in, err)
			}
			var buf bytes.Buffer
			j.MarshalGQL(&buf)
			if got := buf.String(); got != tc.want {
				t.Fatalf("round trip = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestUnmarshalGQL_EvidenceQualityDistributionRoundTrip(t *testing.T) {
	// The exact shape a client would receive for evidenceQualityDistribution
	// and could echo back as a variable -- object with int-valued keys.
	in := map[string]any{
		"high":     float64(3),
		"moderate": float64(5),
		"low":      float64(2),
		"very_low": float64(1),
		"unknown":  float64(0),
	}
	var j JSON
	if err := j.UnmarshalGQL(in); err != nil {
		t.Fatalf("UnmarshalGQL error = %v", err)
	}
	if j.IsNull() {
		t.Fatalf("IsNull() = true, want false for a populated object")
	}
}
