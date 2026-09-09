package benchmarking

import (
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/finite"
)

// TestComputeInternalBaselinesDropsNonFiniteCrossSectionEntries is the
// red-first proof for CHAOS-4806 / ruling R73 at this writer's actual
// non-finite-input reachable surface: a scope whose latest value is a NaN
// (e.g. a zero-denominator ratio computed upstream) must never poison the
// ORG-WIDE percentile cross-section that every OTHER scope's row reports.
// Before this fix, crossSection included the NaN verbatim, so
// Percentile(crossSection, ...) could return NaN for every scope's P25-90
// Value depending on where the NaN landed in sort order -- written straight
// to a plain (non-Nullable) Float64 ClickHouse column, a real "NaN on the
// wire" defect, not a hypothetical one.
func TestComputeInternalBaselinesDropsNonFiniteCrossSectionEntries(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := asOf

	metricName := "test_nonfinite_cross_section_metric"
	series := map[string][]MetricPoint{
		"scope-good-a": {{Day: asOf, Value: 10.0}},
		"scope-good-b": {{Day: asOf, Value: 20.0}},
		"scope-good-c": {{Day: asOf, Value: 30.0}},
		"scope-nan":    {{Day: asOf, Value: math.NaN()}},
	}

	beforeDropped := finite.Count(finiteBaselineFamily, metricName, finite.ReasonNaN)

	baselines := ComputeInternalBaselines(
		metricName, ScopeRepo, series, asOf, computedAt, []int{30}, "org-nonfinite",
	)

	if len(baselines) == 0 {
		t.Fatal("ComputeInternalBaselines returned no rows -- the NaN scope must not refuse the whole computation")
	}

	sawGoodScope, sawNaNScope := false, false
	for _, row := range baselines {
		if row.ScopeKey == "scope-nan" {
			// codex round chaos-4806-r1 P1 fix: the row is NOT dropped --
			// only its own CurrentValue field is nulled.
			sawNaNScope = true
			if row.CurrentValue != nil {
				t.Errorf("scope-nan row CurrentValue = %v, want nil", *row.CurrentValue)
			}
		}
		if row.P25Value == nil || row.P50Value == nil || row.P75Value == nil || row.P90Value == nil {
			t.Fatalf("row for scope %q has a nil percentile field unexpectedly: %+v", row.ScopeKey, row)
		}
		if math.IsNaN(*row.P25Value) || math.IsNaN(*row.P50Value) || math.IsNaN(*row.P75Value) || math.IsNaN(*row.P90Value) {
			t.Fatalf("row for scope %q has a NaN percentile field: %+v -- the dropped scope-nan value leaked into the cross-section percentile", row.ScopeKey, row)
		}
		if math.IsNaN(row.PercentileRank) {
			t.Fatalf("row for scope %q has a NaN PercentileRank: %+v", row.ScopeKey, row)
		}
		if row.ScopeKey == "scope-good-b" {
			sawGoodScope = true
			// Cross-section after dropping the NaN is {10, 20, 30}; p50 of
			// that is 20.
			if *row.P50Value != 20.0 {
				t.Errorf("scope-good-b P50Value = %v, want 20.0 (cross-section with the NaN entry dropped)", *row.P50Value)
			}
		}
	}
	if !sawGoodScope {
		t.Fatal("expected a row for scope-good-b")
	}
	if !sawNaNScope {
		t.Fatal("expected a row for scope-nan (with CurrentValue nulled, not the whole row dropped)")
	}

	afterDropped := finite.Count(finiteBaselineFamily, metricName, finite.ReasonNaN)
	if afterDropped <= beforeDropped {
		t.Errorf("finite.Count(%s,%s,nan) did not increase (before=%d after=%d) -- the boundary trip must be counted", finiteBaselineFamily, metricName, beforeDropped, afterDropped)
	}
}

// TestComputeInternalBaselinesDropsNonFiniteWindowValues proves the second
// boundary in this file: a scope's own rolling-window history can carry a
// non-finite sample without corrupting that scope's BaselineValue (Mean),
// while the row itself still writes with its other fields intact.
func TestComputeInternalBaselinesDropsNonFiniteWindowValues(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := asOf
	metricName := "test_nonfinite_window_metric"

	series := map[string][]MetricPoint{
		"scope-mixed": {
			{Day: asOf.AddDate(0, 0, -2), Value: 4.0},
			{Day: asOf.AddDate(0, 0, -1), Value: math.Inf(1)},
			{Day: asOf, Value: 6.0},
		},
	}

	baselines := ComputeInternalBaselines(
		metricName, ScopeRepo, series, asOf, computedAt, []int{30}, "org-nonfinite",
	)
	if len(baselines) != 1 {
		t.Fatalf("ComputeInternalBaselines returned %d rows, want 1", len(baselines))
	}
	row := baselines[0]
	if row.BaselineValue == nil {
		t.Fatal("BaselineValue = nil, want a finite mean of {4.0, 6.0} = 5.0 (the +Inf sample dropped, two real samples remain)")
	}
	if math.IsInf(*row.BaselineValue, 0) || math.IsNaN(*row.BaselineValue) {
		t.Fatalf("BaselineValue = %v, want a finite mean of {4.0, 6.0} = 5.0 (the +Inf sample dropped)", *row.BaselineValue)
	}
	if *row.BaselineValue != 5.0 {
		t.Errorf("BaselineValue = %v, want 5.0", *row.BaselineValue)
	}
}

// TestComputeInternalBaselinesAllNonFiniteWindowNullsNotZero is the
// red-first proof for codex round chaos-4806-r1 P1 finding 2: an
// all-non-finite window (every point dropped) must record BaselineValue as
// undefined (nil), never fall through to Mean(nil)'s own documented 0.0 --
// 0.0 silently standing in for undefined is exactly what R73 bans.
func TestComputeInternalBaselinesAllNonFiniteWindowNullsNotZero(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	metricName := "test_all_nonfinite_window_metric"

	series := map[string][]MetricPoint{
		"scope-all-bad": {
			{Day: asOf.AddDate(0, 0, -1), Value: math.NaN()},
			{Day: asOf, Value: math.Inf(-1)},
		},
	}

	beforeUndefined := finite.Count(finiteBaselineFamily, metricName, finite.ReasonUndefinedInput)

	baselines := ComputeInternalBaselines(
		metricName, ScopeRepo, series, asOf, asOf, []int{30}, "org-nonfinite",
	)
	if len(baselines) != 1 {
		t.Fatalf("ComputeInternalBaselines returned %d rows, want 1 (a window with real points, even all-non-finite ones, still writes a row)", len(baselines))
	}
	row := baselines[0]
	if row.BaselineValue != nil {
		t.Fatalf("BaselineValue = %v, want nil (every window point was non-finite; Mean(nil)'s 0.0 default must not leak through as a stand-in for undefined)", *row.BaselineValue)
	}
	// This fixture's own latest value (asOf's own point, -Inf) is ALSO
	// non-finite, so CurrentValue is independently nil here too (a separate
	// boundary trip, counted under its own real reason -- negative_infinity
	// -- by the CurrentValue path tested elsewhere); nothing further to
	// assert about it in this test, which targets BaselineValue only.

	afterUndefined := finite.Count(finiteBaselineFamily, metricName, finite.ReasonUndefinedInput)
	if afterUndefined <= beforeUndefined {
		t.Errorf("finite.Count(%s,%s,undefined_input) did not increase (before=%d after=%d) -- the all-non-finite-window trip must be counted", finiteBaselineFamily, metricName, beforeUndefined, afterUndefined)
	}
}

// TestComputeInternalBaselinesPercentileOverflowNullsNotInf is the
// red-first proof for codex round chaos-4806-r1 P1 finding 1: a percentile
// computed over FINITE inputs can still overflow to +Inf (IEEE 754 double
// range limits, not a bad input) -- the RESULT, not just the inputs, must
// be validated at this write boundary. -MaxFloat64 and +MaxFloat64 are both
// perfectly ordinary finite float64 values; DropNonFinite lets both through
// unchanged, and the interpolation step's own subtraction
// (upperValue-lowerValue, both near +-MaxFloat64) overflows before any
// non-finite value was ever an input.
func TestComputeInternalBaselinesPercentileOverflowNullsNotInf(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	metricName := "test_percentile_overflow_metric"

	series := map[string][]MetricPoint{
		"scope-min": {{Day: asOf, Value: -math.MaxFloat64}},
		"scope-max": {{Day: asOf, Value: math.MaxFloat64}},
	}

	beforeInf := finite.Count(finiteBaselineFamily, metricName, finite.ReasonPositiveInf)

	baselines := ComputeInternalBaselines(
		metricName, ScopeRepo, series, asOf, asOf, []int{30}, "org-nonfinite",
	)
	if len(baselines) == 0 {
		t.Fatal("expected at least one row")
	}
	for _, row := range baselines {
		for name, ptr := range map[string]*float64{
			"P25Value": row.P25Value, "P50Value": row.P50Value,
			"P75Value": row.P75Value, "P90Value": row.P90Value,
		} {
			if ptr != nil && (math.IsInf(*ptr, 0) || math.IsNaN(*ptr)) {
				t.Fatalf("%s = %v for scope %q, want nil or a finite value -- an overflowed percentile result reached the wire", name, *ptr, row.ScopeKey)
			}
		}
	}

	afterInf := finite.Count(finiteBaselineFamily, metricName, finite.ReasonPositiveInf)
	if afterInf <= beforeInf {
		t.Errorf("finite.Count(%s,%s,positive_infinity) did not increase (before=%d after=%d) -- an overflowed percentile result must be counted", finiteBaselineFamily, metricName, beforeInf, afterInf)
	}
}

// TestComputeInternalBaselinesMeanOverflowNullsNotInf is codex round
// chaos-4806-r1 P1 finding 1's second reproduction: Mean's own summation of
// two finite MaxFloat64 window points overflows to +Inf before DropNonFinite
// (an input-side filter) ever gets a chance to see anything wrong -- the
// RESULT must be validated too.
func TestComputeInternalBaselinesMeanOverflowNullsNotInf(t *testing.T) {
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	metricName := "test_mean_overflow_metric"

	series := map[string][]MetricPoint{
		"scope-overflow": {
			{Day: asOf.AddDate(0, 0, -1), Value: math.MaxFloat64},
			{Day: asOf, Value: math.MaxFloat64},
		},
	}

	beforeInf := finite.Count(finiteBaselineFamily, metricName, finite.ReasonPositiveInf)

	baselines := ComputeInternalBaselines(
		metricName, ScopeRepo, series, asOf, asOf, []int{30}, "org-nonfinite",
	)
	if len(baselines) != 1 {
		t.Fatalf("ComputeInternalBaselines returned %d rows, want 1", len(baselines))
	}
	row := baselines[0]
	if row.BaselineValue != nil && (math.IsInf(*row.BaselineValue, 0) || math.IsNaN(*row.BaselineValue)) {
		t.Fatalf("BaselineValue = %v, want nil or finite -- Mean's overflowed sum reached the wire", *row.BaselineValue)
	}

	afterInf := finite.Count(finiteBaselineFamily, metricName, finite.ReasonPositiveInf)
	if afterInf <= beforeInf {
		t.Errorf("finite.Count(%s,%s,positive_infinity) did not increase (before=%d after=%d) -- an overflowed Mean result must be counted", finiteBaselineFamily, metricName, beforeInf, afterInf)
	}
}
