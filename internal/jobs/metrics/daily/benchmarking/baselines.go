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

// nullableRound4 is this file's R73 write boundary for a single scalar
// aggregate result (a Percentile/Mean output, or a scope's own latest
// value): finite -> a pointer to its round4'd value; non-finite (NaN, or an
// overflow to +-Inf on an extreme-but-finite input -- Codex round chaos-
// 4806-r1 P1: a percentile/mean over finite inputs can still overflow) ->
// the trip is recorded (family/field, the REAL reason) and nil is returned,
// for a Nullable(Float64) column (see migration 090_testops_baselines_
// nullable_fields.sql) to write ClickHouse NULL.
func nullableRound4(family, field string, value float64) *float64 {
	safe := finite.NullIfNonFinite(family, field, value)
	if safe == nil {
		return nil
	}
	rounded := round4(*safe)
	return &rounded
}

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
	// codex round chaos-4806-r3b P3 (mutation-tested finding): every call in
	// this function used the bare metricName as its finite.* field tag, so
	// all four undefined_input call sites below were indistinguishable
	// telemetry -- deleting any ONE of them left every test's `finite.Count`
	// assertion satisfied by the other three firing for the same fixture.
	// Suffix the field per DISTINCT boundary so each is independently
	// observable (both in production telemetry and in tests, which can now
	// assert an exact count on their own specific field instead of a
	// same-key total).
	crossSectionField := metricName + ":cross_section"
	percentileValuesField := metricName + ":percentile_values"
	currentValueField := metricName + ":current_value"
	percentileRankField := metricName + ":percentile_rank"
	windowField := metricName + ":window"
	baselineValueField := metricName + ":baseline_value"

	crossSection = finite.DropNonFinite(finiteBaselineFamily, crossSectionField, crossSection)

	// CHAOS-4806 / ruling R73, codex round chaos-4806-r1 P1 (executed repro:
	// a cross-section of finite MaxFloat64-magnitude values produces a +Inf
	// Percentile result -- an overflow, not a bad INPUT, so DropNonFinite
	// above cannot catch it). Percentile's own RESULT is validated here too,
	// same as every other scalar this file writes -- nullableRound4 records
	// the trip and nils the field rather than writing +-Inf.
	//
	// codex round chaos-4806-r2 P1 (executed repro: when EVERY cross-
	// section value was non-finite, crossSection is empty here, and
	// Percentile(nil, pct) returns its own documented 0.0-for-empty-input
	// default -- a perfectly finite number, so nullableRound4 could not
	// tell it apart from a genuinely computed 0.0 and wrote it as real
	// data. Checked explicitly: an empty cross-section means "no scope in
	// this metric window has a usable value at all," which is undefined,
	// not zero.
	var p25, p50, p75, p90 *float64
	if len(crossSection) == 0 {
		finite.Undefined(finiteBaselineFamily, percentileValuesField)
	} else {
		p25 = nullableRound4(finiteBaselineFamily, percentileValuesField, Percentile(crossSection, 25.0))
		p50 = nullableRound4(finiteBaselineFamily, percentileValuesField, Percentile(crossSection, 50.0))
		p75 = nullableRound4(finiteBaselineFamily, percentileValuesField, Percentile(crossSection, 75.0))
		p90 = nullableRound4(finiteBaselineFamily, percentileValuesField, Percentile(crossSection, 90.0))
	}

	var results []BenchmarkBaselineRecord
	for _, scopeKey := range scopeKeys {
		latestValue, ok := currentValues[scopeKey]
		if !ok {
			continue
		}
		// CHAOS-4806 / ruling R73, codex round chaos-4806-r1 P1 (executed
		// repro: skipping this scope's rows entirely dropped a row R73 says
		// must still write, and mis-tagged the boundary trip as
		// ReasonUndefinedInput instead of the real NaN/+-Inf reason).
		// CurrentValue is now Nullable(Float64) (migration
		// 090_testops_baselines_nullable_fields.sql) -- keep the row, null
		// ONLY this one field via nullableRound4, which records the actual
		// reason through finite.Check.
		currentValue := nullableRound4(finiteBaselineFamily, currentValueField, latestValue)
		// codex round chaos-4806-r3b P1 (executed repro): PercentileRank
		// was computed from the RAW latestValue even when currentValue
		// above came back nil -- safe for a NaN comparison value (every
		// comparison against NaN is false, so PercentileRank(cs, NaN)
		// always lands at 0), but NOT for +-Inf: every finite candidate
		// genuinely compares less-than +Inf (greater-than -Inf), so an
		// undefined scope could still rank a fully confident 100 (or 0)
		// -- "leading" maturity, 1.0 confidence, for data that is
		// undefined. If this scope's own value is non-finite, its
		// PercentileRank is undefined too (migration
		// 092_testops_percentile_rank_nullable_field.sql); only compute
		// the real rank when currentValue survived the check above.
		var percentileRank *float64
		if currentValue == nil {
			finite.Undefined(finiteBaselineFamily, percentileRankField)
		} else {
			// currentValue != nil means latestValue passed the same finite
			// check DropNonFinite used to build crossSection above, so THIS
			// scope's own value necessarily survived into it -- crossSection
			// is therefore non-empty here by construction (it contains at
			// least this scope's own entry), never the empty-cohort case.
			percentileRank = nullableRound4(finiteBaselineFamily, percentileRankField, PercentileRank(crossSection, latestValue))
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
			// BaselineValue is now Nullable(Float64) too. This window had
			// real points, so the row still gets written regardless of
			// what's left after filtering.
			values = finite.DropNonFinite(finiteBaselineFamily, windowField, values)
			// CHAOS-4806 / ruling R73, codex round chaos-4806-r1 P1
			// (executed repro: an all-non-finite window fell through to
			// Mean(nil)'s own documented 0.0-for-empty-input default --
			// 0.0 silently standing in for undefined, exactly what R73
			// bans). An empty `values` here means every point in this
			// window was non-finite (DropNonFinite already recorded each
			// drop) -- record the mean itself as undefined rather than
			// asking Mean to answer a question it has no data for.
			var baselineValue *float64
			if len(values) == 0 {
				finite.Undefined(finiteBaselineFamily, baselineValueField)
			} else {
				baselineValue = nullableRound4(finiteBaselineFamily, baselineValueField, Mean(values))
			}
			periodStart := asOfDay.AddDate(0, 0, -(windowDays - 1))
			results = append(results, BenchmarkBaselineRecord{
				MetricName:        metricName,
				ScopeType:         scopeType,
				ScopeKey:          scopeKey,
				PeriodStart:       periodStart,
				PeriodEnd:         asOfDay,
				RollingWindowDays: windowDays,
				CurrentValue:      currentValue,
				BaselineValue:     baselineValue,
				PercentileRank:    percentileRank,
				P25Value:          p25,
				P50Value:          p50,
				P75Value:          p75,
				P90Value:          p90,
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
		// CHAOS-4806 / ruling R73, codex round chaos-4806-r3b P1: a nil
		// PercentileRank means "no cohort to rank against" or "this
		// scope's own value is undefined" -- there is no band/confidence
		// to classify without a rank, so this baseline row produces no
		// maturity-band row at all (the baseline row itself still wrote,
		// with PercentileRank correctly NULL -- this is a "nothing to
		// derive" skip, the same shape as windowValues' own empty-window
		// skip elsewhere in this file, not a silent substitution).
		if baseline.PercentileRank == nil {
			continue
		}
		rank := *baseline.PercentileRank
		records = append(records, MaturityBandRecord{
			MetricName:     baseline.MetricName,
			ScopeType:      baseline.ScopeType,
			ScopeKey:       baseline.ScopeKey,
			PeriodStart:    baseline.PeriodStart,
			PeriodEnd:      baseline.PeriodEnd,
			Value:          baseline.CurrentValue,
			PercentileRank: rank,
			MaturityBand:   bandForPercentile(rank),
			Confidence:     confidenceForPercentile(rank),
			ComputedAt:     baseline.ComputedAt,
			OrgID:          baseline.OrgID,
		})
	}
	return records
}
