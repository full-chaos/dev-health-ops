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

	sawGoodScope := false
	for _, row := range baselines {
		if row.ScopeKey == "scope-nan" {
			t.Errorf("scope-nan produced a row (%+v) -- its own latest value is NaN and out of this fix's scope (see ComputeInternalBaselines' own doc note); this test only asserts it doesn't poison OTHER scopes", row)
		}
		if math.IsNaN(row.P25Value) || math.IsNaN(row.P50Value) || math.IsNaN(row.P75Value) || math.IsNaN(row.P90Value) {
			t.Fatalf("row for scope %q has a NaN percentile field: %+v -- the dropped scope-nan value leaked into the cross-section percentile", row.ScopeKey, row)
		}
		if math.IsNaN(row.PercentileRank) {
			t.Fatalf("row for scope %q has a NaN PercentileRank: %+v", row.ScopeKey, row)
		}
		if row.ScopeKey == "scope-good-b" {
			sawGoodScope = true
			// Cross-section after dropping the NaN is {10, 20, 30}; p50 of
			// that is 20.
			if row.P50Value != 20.0 {
				t.Errorf("scope-good-b P50Value = %v, want 20.0 (cross-section with the NaN entry dropped)", row.P50Value)
			}
		}
	}
	if !sawGoodScope {
		t.Fatal("expected a row for scope-good-b")
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
	if math.IsInf(row.BaselineValue, 0) || math.IsNaN(row.BaselineValue) {
		t.Fatalf("BaselineValue = %v, want a finite mean of {4.0, 6.0} = 5.0 (the +Inf sample dropped)", row.BaselineValue)
	}
	if row.BaselineValue != 5.0 {
		t.Errorf("BaselineValue = %v, want 5.0", row.BaselineValue)
	}
}
