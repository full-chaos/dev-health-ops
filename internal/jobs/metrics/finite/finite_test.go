package finite

import (
	"math"
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	cases := []struct {
		name   string
		value  float64
		reason Reason
		ok     bool
	}{
		{"finite zero", 0.0, "", true},
		{"finite negative", -3.5, "", true},
		{"nan", math.NaN(), ReasonNaN, false},
		{"positive infinity", math.Inf(1), ReasonPositiveInf, false},
		{"negative infinity", math.Inf(-1), ReasonNegativeInf, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := Check(tc.value)
			if ok != tc.ok || reason != tc.reason {
				t.Errorf("Check(%v) = (%q, %v), want (%q, %v)", tc.value, reason, ok, tc.reason, tc.ok)
			}
		})
	}
}

func TestNullIfNonFiniteFinitePassesThrough(t *testing.T) {
	got := NullIfNonFinite("t_family", "t_field_finite", 42.5)
	if got == nil || *got != 42.5 {
		t.Fatalf("NullIfNonFinite(finite) = %v, want pointer to 42.5", got)
	}
}

// TestNullIfNonFiniteNullsAndCounts is the red-first proof for the row
// writer half of R73: a NaN reaching this boundary must become nil (write
// ClickHouse NULL for that field only), never propagate, and the trip must
// be counted so it is observable, not just silently swallowed.
func TestNullIfNonFiniteNullsAndCounts(t *testing.T) {
	family, field := "t_family_writer", "t_field_nan"
	before := Count(family, field, ReasonNaN)

	got := NullIfNonFinite(family, field, math.NaN())
	if got != nil {
		t.Fatalf("NullIfNonFinite(NaN) = %v, want nil", *got)
	}

	after := Count(family, field, ReasonNaN)
	if after != before+1 {
		t.Fatalf("Count(%s,%s,nan) = %d, want %d (before %d + 1 trip)", family, field, after, before+1, before)
	}
}

func TestNullIfNonFiniteInfinitiesNullAndCountSeparately(t *testing.T) {
	family, field := "t_family_writer", "t_field_inf"
	beforePos := Count(family, field, ReasonPositiveInf)
	beforeNeg := Count(family, field, ReasonNegativeInf)

	if got := NullIfNonFinite(family, field, math.Inf(1)); got != nil {
		t.Fatalf("NullIfNonFinite(+Inf) = %v, want nil", *got)
	}
	if got := NullIfNonFinite(family, field, math.Inf(-1)); got != nil {
		t.Fatalf("NullIfNonFinite(-Inf) = %v, want nil", *got)
	}

	if got := Count(family, field, ReasonPositiveInf); got != beforePos+1 {
		t.Errorf("Count(+Inf) = %d, want %d", got, beforePos+1)
	}
	if got := Count(family, field, ReasonNegativeInf); got != beforeNeg+1 {
		t.Errorf("Count(-Inf) = %d, want %d", got, beforeNeg+1)
	}
}

func TestUndefinedRecordsReasonAndReturnsNil(t *testing.T) {
	family, field := "t_family_avg", "t_field_empty"
	before := Count(family, field, ReasonUndefinedInput)

	got := Undefined(family, field)
	if got != nil {
		t.Fatalf("Undefined() = %v, want nil", *got)
	}
	if after := Count(family, field, ReasonUndefinedInput); after != before+1 {
		t.Fatalf("Count(undefined_input) = %d, want %d", after, before+1)
	}
}

// TestJSONLiteralNeverEmitsNonFiniteTokens is the wire-level red-first proof
// for the JSON-serialization half of R73: scans the RENDERED output, not a
// hand list of forbidden strings, so a future render func that reintroduces
// "NaN"/"Infinity" text some other way still fails this test.
func TestJSONLiteralNeverEmitsNonFiniteTokens(t *testing.T) {
	forbidden := []string{"NaN", "Infinity", "-Infinity"}
	render := func(v float64) string {
		// A deliberately naive renderer, the same shape as the pre-fix
		// pythonFloatJSON: if the boundary did not intercept first, this
		// would leak the bad token straight onto the wire.
		if math.IsNaN(v) {
			return "NaN"
		}
		if math.IsInf(v, 1) {
			return "Infinity"
		}
		if math.IsInf(v, -1) {
			return "-Infinity"
		}
		return "finite"
	}

	inputs := []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, v := range inputs {
		got := JSONLiteral("t_family_json", "t_field", v, render)
		if got != "null" {
			t.Errorf("JSONLiteral(%v) = %q, want \"null\"", v, got)
		}
		for _, bad := range forbidden {
			if strings.Contains(got, bad) {
				t.Errorf("JSONLiteral(%v) = %q contains forbidden wire token %q", v, got, bad)
			}
		}
	}
}

func TestJSONLiteralFinitePassesThroughToRender(t *testing.T) {
	got := JSONLiteral("t_family_json", "t_field", 3.0, func(v float64) string { return "3.0" })
	if got != "3.0" {
		t.Fatalf("JSONLiteral(finite) = %q, want \"3.0\"", got)
	}
}

// TestDropNonFiniteDropsAndCountsWithoutRefusingTheRest is the red-first
// proof for the percentile/aggregate helper half of R73: a poisoned input
// slice must lose only its bad entries, never the whole computation.
func TestDropNonFiniteDropsAndCountsWithoutRefusingTheRest(t *testing.T) {
	family, field := "t_family_percentile", "t_field_series"
	beforeNaN := Count(family, field, ReasonNaN)
	beforePos := Count(family, field, ReasonPositiveInf)

	values := []float64{1.0, math.NaN(), 2.0, math.Inf(1), 3.0}
	got := DropNonFinite(family, field, values)

	want := []float64{1.0, 2.0, 3.0}
	if len(got) != len(want) {
		t.Fatalf("DropNonFinite(...) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DropNonFinite(...) = %v, want %v", got, want)
		}
	}
	if after := Count(family, field, ReasonNaN); after != beforeNaN+1 {
		t.Errorf("Count(nan) = %d, want %d", after, beforeNaN+1)
	}
	if after := Count(family, field, ReasonPositiveInf); after != beforePos+1 {
		t.Errorf("Count(+inf) = %d, want %d", after, beforePos+1)
	}
}

func TestDropNonFiniteAllBadReturnsEmptyNotNil(t *testing.T) {
	got := DropNonFinite("t_family_percentile", "t_field_all_bad", []float64{math.NaN(), math.Inf(1)})
	if len(got) != 0 {
		t.Fatalf("DropNonFinite(all non-finite) = %v, want empty", got)
	}
}

func TestWritePrometheusIncludesObservedCounters(t *testing.T) {
	NullIfNonFinite("t_family_prom", "t_field_prom", math.NaN())

	var buf strings.Builder
	if err := MetricsSource().WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "dev_health_metrics_finite_boundary_nonfinite_total") {
		t.Fatalf("WritePrometheus output missing metric name: %q", out)
	}
	if !strings.Contains(out, `family="t_family_prom"`) || !strings.Contains(out, `field="t_field_prom"`) || !strings.Contains(out, `reason="nan"`) {
		t.Fatalf("WritePrometheus output missing observed labels: %q", out)
	}
}

func TestWritePrometheusNilReceiverIsNoop(t *testing.T) {
	var m *Metrics
	if err := m.WritePrometheus(&strings.Builder{}); err != nil {
		t.Fatalf("WritePrometheus on nil receiver: %v", err)
	}
}
