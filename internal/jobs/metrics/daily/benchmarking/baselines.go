package benchmarking

import (
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/finite"
)

// finiteBaselineFamily tags every internal/jobs/metrics/finite boundary trip
// this file records, so the counter (CHAOS-4806, ruling R73) can tell a
// baseline-percentile drop apart from any other family's.
const finiteBaselineFamily = "benchmarking_baseline"

// DefaultBaselineWindows mirrors compute_internal_baselines' `windows` default
// (baselines.py:39).
var DefaultBaselineWindows = []int{30, 60, 90}

// latestValueOnOrBefore ports baselines.py:17-23.
//
// Python takes `eligible[-1]` -- the LAST eligible point in the series' own
// order, NOT the maximum by day. The loader's query carries `ORDER BY
// scope_key, day` (_common.py:327) so in practice that is the latest day, but
// the compute does not sort, and neither does this. Mirroring the ordering
// dependency rather than "fixing" it keeps the golden honest: a caller passing
// unordered points gets Python's answer, whatever that is.
func latestValueOnOrBefore(points []MetricPoint, asOfDay time.Time) (float64, bool) {
	var latest float64
	found := false
	for _, point := range points {
		if !point.Day.After(asOfDay) {
			latest = point.Value
			found = true
		}
	}
	return latest, found
}

// windowValues ports baselines.py:26-31: values within [as_of - (window-1), as_of].
func windowValues(points []MetricPoint, asOfDay time.Time, windowDays int) []float64 {
	startDay := asOfDay.AddDate(0, 0, -(windowDays - 1))
	var values []float64
	for _, point := range points {
		if !point.Day.Before(startDay) && !point.Day.After(asOfDay) {
			values = append(values, point.Value)
		}
	}
	return values
}

// ComputeInternalBaselines ports compute_internal_baselines (baselines.py:34-91).
//
// The cross-section is built from every scope's latest value; the four
// percentiles are computed ONCE over that cross-section and repeated on every
// emitted row, so a scope's p25/p50/p75/p90 describe the org, not the scope.
// Row order is `sorted(series_by_scope.items())` -- scope_key ascending -- with
// the window loop inside.
func ComputeInternalBaselines(
	metricName string,
	scopeType string,
	seriesByScope map[string][]MetricPoint,
	asOfDay time.Time,
	computedAt time.Time,
	windows []int,
	orgID string,
) []BenchmarkBaselineRecord {
	currentValues := make(map[string]float64, len(seriesByScope))
	for scopeKey, points := range seriesByScope {
		if latest, ok := latestValueOnOrBefore(points, asOfDay); ok {
			currentValues[scopeKey] = latest
		}
	}
	if len(currentValues) == 0 {
		return nil
	}

	// ORDERING NOTE: Python builds `cross_section_values` as
	// `list(current_values_by_scope.values())` -- dict insertion order, which
	// is the iteration order of series_by_scope. percentile() sorts its input,
	// so the four percentiles are order-INSENSITIVE; percentile_rank only
	// counts; and sample_size is a length. So the cross-section's order cannot
	// affect any output, and building it from sorted keys here is a
	// determinism improvement with no behavioural difference.
	scopeKeys := make([]string, 0, len(seriesByScope))
	for scopeKey := range seriesByScope {
		scopeKeys = append(scopeKeys, scopeKey)
	}
	sort.Strings(scopeKeys)

	crossSection := make([]float64, 0, len(currentValues))
	for _, scopeKey := range scopeKeys {
		if value, ok := currentValues[scopeKey]; ok {
			crossSection = append(crossSection, value)
		}
	}

	// CHAOS-4806 / ruling R73: a NaN or +-Inf metric value (e.g. a
	// zero-denominator ratio upstream) must never propagate through a
	// percentile or a mean into P25-P90Value/PercentileRank/BaselineValue --
	// these columns are plain (non-Nullable) Float64 in ClickHouse today, so
	// a NaN reaching them would be written to the wire as a real, silent
	// non-finite value, not caught by any Nullable-aware guard. Dropping the
	// bad entries here, before Percentile/PercentileRank/Mean ever see them,
	// keeps every OTHER scope's percentile/mean computing over real data
	// (the row, the family, the window all still write) instead of letting
	// one poisoned scope corrupt the whole cross-section. Each drop is
	// counted (family=benchmarking_baseline, field=metricName) so a
	// non-zero rate is a visible data-quality signal, not a silent skip --
	// same shape as this package's existing observeNonFinitePercentileInput
	// counter, but this one actually removes the bad input rather than only
	// observing it, per R73's explicit "correctness over parity" ruling
	// (Python parity is not required here; see Percentile's own doc comment
	// for why its raw NaN-ordering behavior is deliberately left untouched).
	crossSection = finite.DropNonFinite(finiteBaselineFamily, metricName, crossSection)

	p25 := Percentile(crossSection, 25.0)
	p50 := Percentile(crossSection, 50.0)
	p75 := Percentile(crossSection, 75.0)
	p90 := Percentile(crossSection, 90.0)

	var results []BenchmarkBaselineRecord
	for _, scopeKey := range scopeKeys {
		latestValue, ok := currentValues[scopeKey]
		if !ok {
			continue
		}
		if _, isFinite := finite.Check(latestValue); !isFinite {
			// CHAOS-4806 / ruling R73: CurrentValue is a plain (non-
			// Nullable) Float64 column, so there is no NULL to write for
			// this ONE field -- and writing the NaN/+-Inf itself is exactly
			// what R73 bans. Skipping only THIS scope's rows (every window)
			// is the schema-constrained equivalent of nulling the field:
			// every OTHER scope's rows, and this metric's org-wide
			// percentile cross-section (already filtered above), keep
			// computing and writing untouched -- the window is not
			// wrecked, only this one un-computable scope is absent from it.
			finite.Undefined(finiteBaselineFamily, metricName)
			continue
		}
		points := seriesByScope[scopeKey]
		for _, windowDays := range windows {
			values := windowValues(points, asOfDay, windowDays)
			// windowValues.py parity: a window with literally no points in
			// range still skips the row entirely (nothing to compute at
			// all, same as before this fix).
			if len(values) == 0 {
				continue
			}
			// CHAOS-4806 / ruling R73: same boundary as crossSection above,
			// applied to this scope's own window before Mean() --
			// BaselineValue is a plain Float64 column too. Deliberately
			// checked for emptiness BEFORE this filter (above) but not
			// after: a window that had real points, all of which happened
			// to be non-finite, still keeps this row (CurrentValue/
			// PercentileRank/P25-90Value come from crossSection and this
			// scope's own latestValue, none of which depend on `values`) --
			// Mean(nil) already returns its own documented 0.0 for an empty
			// input, so BaselineValue degrades the same way Percentile's
			// empty-input case already does elsewhere in this file, rather
			// than silently dropping an otherwise-good row.
			values = finite.DropNonFinite(finiteBaselineFamily, metricName, values)
			periodStart := asOfDay.AddDate(0, 0, -(windowDays - 1))
			results = append(results, BenchmarkBaselineRecord{
				MetricName:        metricName,
				ScopeType:         scopeType,
				ScopeKey:          scopeKey,
				PeriodStart:       periodStart,
				PeriodEnd:         asOfDay,
				RollingWindowDays: windowDays,
				CurrentValue:      round4(latestValue),
				BaselineValue:     round4(Mean(values)),
				PercentileRank:    round4(PercentileRank(crossSection, latestValue)),
				P25Value:          round4(p25),
				P50Value:          round4(p50),
				P75Value:          round4(p75),
				P90Value:          round4(p90),
				SampleSize:        len(crossSection),
				ComputedAt:        computedAt,
				OrgID:             orgID,
			})
		}
	}
	return results
}

// bandForPercentile ports maturity.py:12-19. The comparisons are strict `<`,
// so a rank exactly ON a boundary lands in the HIGHER band.
func bandForPercentile(percentileRank float64) string {
	if percentileRank < 25.0 {
		return "emerging"
	}
	if percentileRank < 50.0 {
		return "developing"
	}
	if percentileRank < 75.0 {
		return "established"
	}
	return "leading"
}

// confidenceForPercentile ports maturity.py:22-26: distance to the nearest
// band boundary, capped at 25 and rescaled into [0.5, 1.0].
func confidenceForPercentile(percentileRank float64) float64 {
	boundaries := []float64{25.0, 50.0, 75.0}
	distance := abs(percentileRank - boundaries[0])
	for _, boundary := range boundaries[1:] {
		if candidate := abs(percentileRank - boundary); candidate < distance {
			distance = candidate
		}
	}
	scaled := distance
	if scaled > 25.0 {
		scaled = 25.0
	}
	scaled = scaled / 25.0
	// `0.5 + (scaled * 0.5)` is a multiply feeding an add -- an FMA candidate.
	// The barrier goes on the PRODUCT-valued variable, which is where the
	// fusion actually happens (see Percentile's doc comment).
	half := float64(scaled * 0.5)
	return round4(float64(0.5 + half))
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

// ClassifyMaturityBands ports classify_maturity_bands (maturity.py:29-50).
//
// It reads the baseline's ALREADY-ROUNDED percentile_rank and current_value,
// and carries the baseline's own computed_at and org_id -- the `computed_at`
// parameter Python accepts is explicitly discarded (`del computed_at`,
// maturity.py:32), so it is not a parameter here either.
func ClassifyMaturityBands(baselines []BenchmarkBaselineRecord) []MaturityBandRecord {
	if len(baselines) == 0 {
		return nil
	}
	records := make([]MaturityBandRecord, 0, len(baselines))
	for _, baseline := range baselines {
		records = append(records, MaturityBandRecord{
			MetricName:     baseline.MetricName,
			ScopeType:      baseline.ScopeType,
			ScopeKey:       baseline.ScopeKey,
			PeriodStart:    baseline.PeriodStart,
			PeriodEnd:      baseline.PeriodEnd,
			Value:          baseline.CurrentValue,
			PercentileRank: baseline.PercentileRank,
			MaturityBand:   bandForPercentile(baseline.PercentileRank),
			Confidence:     confidenceForPercentile(baseline.PercentileRank),
			ComputedAt:     baseline.ComputedAt,
			OrgID:          baseline.OrgID,
		})
	}
	return records
}
