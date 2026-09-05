package benchmarking

import (
	"math"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// aliasMetrics mirrors _common.py's ALIAS_METRICS.
var aliasMetrics = map[string]string{
	"pipeline_success": "success_rate",
	"defect_rate":      "defect_intro_rate",
}

// negativeDirectionMetrics mirrors _common.py's NEGATIVE_DIRECTION_METRICS:
// metrics where a DECREASE is an improvement.
var negativeDirectionMetrics = map[string]bool{
	"failure_rate":              true,
	"flake_rate":                true,
	"retry_dependency_rate":     true,
	"failure_recurrence_score":  true,
	"rerun_rate":                true,
	"median_duration_seconds":   true,
	"p95_duration_seconds":      true,
	"avg_queue_seconds":         true,
	"p95_queue_seconds":         true,
	"coverage_regression_count": true,
	"cycle_time_hours":          true,
	"defect_intro_rate":         true,
}

// CanonicalMetricName resolves an alias to its underlying metric name.
func CanonicalMetricName(metricName string) string {
	if canonical, ok := aliasMetrics[metricName]; ok {
		return canonical
	}
	return metricName
}

// MetricIsNegative reports whether a DECREASE in this metric is an
// improvement. Note it canonicalises first, so "defect_rate" and
// "defect_intro_rate" agree.
func MetricIsNegative(metricName string) bool {
	return negativeDirectionMetrics[CanonicalMetricName(metricName)]
}

// Percentile ports _common.py:167-179.
//
// # THE FMA BARRIER IS ON `rank`, AND THAT IS NOT WHERE YOU WOULD LOOK
//
// The obvious arm64 FMA candidate here is the interpolation
// `lower + (upper-lower)*weight` -- a textbook `x*y + z`. MEASURED, it is not
// the problem: that expression agrees with CPython bit-for-bit whether written
// fused, barriered, or as an explicit math.FMA.
//
// The site that actually diverges is `rank - math.Floor(rank)`, which contains
// no visible multiply at all. `rank` is itself a product, `float64(n-1) *
// (pct/100.0)`, and the compiler inlines that definition into the subtraction,
// producing FMA(n-1, pct/100, -floor) -- one rounding where CPython does two.
// Measured on this corpus:
//
//	weight, fused    0x3fd999999999999c   (0.40000000000000013)
//	weight, barrier  0x3fd99999999999a0   (0.40000000000000036)  == CPython
//
// which shifts p90 by one ulp and, through percentile_rank, can move a scope
// into a different maturity BAND.
//
// The lesson generalises: an FMA site is not always the statement containing
// the `*`. Any float variable whose DEFINITION is a product can be fused into
// a later add or subtract, however innocent that later statement looks. The
// explicit float64 conversion on `rank` is what forces the intermediate
// rounding; it is load-bearing and must not be "simplified" away.
//
// Returns 0 for an empty slice; Python raises, but every caller here guards on
// emptiness first and a panic in a metrics job is worse than a defined zero.
func Percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	observeNonFinitePercentileInput(values)
	ordered := make([]float64, len(values))
	copy(ordered, values)
	// sort.SliceStable with a plain `a < b`, NOT sort.Float64s (CHAOS-4288,
	// codex r1 on #2235).
	//
	// sort.Float64s sorts NaN to the FRONT. CPython's `sorted` moves it
	// nowhere, because every comparison against NaN is False, so the two
	// disagree on the simplest possible input:
	//
	//	[1, NaN, 2]   CPython -> [1, NaN, 2]      sort.Float64s -> [NaN, 1, 2]
	//
	// and the percentile then differs. SliceStable with the same comparator
	// CPython uses agrees on every single-NaN placement measured.
	//
	// KNOWN RESIDUAL, stated because it is NOT fixed here: CPython's ordering
	// under NaN is a Timsort ARTEFACT, not a specification -- its comparator is
	// non-transitive, so `sorted([2.0, nan, 1.0])` returns [2.0, nan, 1.0],
	// which is not sorted at all. No stable sort reproduces that in general,
	// and the planes diverge once more than one element sits on each side:
	//
	//	[3, 1, NaN, 2]  CPython -> [1, 2, 3, NaN]   SliceStable -> [1, 3, NaN, 2]
	//
	// Matching it exactly would mean reimplementing Timsort, i.e. parity with
	// an artefact. The real fix is upstream -- neither plane should be taking a
	// percentile over a series containing NaN at all -- and that is ticketed
	// against BOTH planes. countNonFinite below makes the input visible when it
	// happens; it deliberately does NOT change the result, because a Go-only
	// skip would be a new divergence rather than a fix.
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered) == 1 {
		return ordered[0]
	}
	rank := float64(float64(len(ordered)-1) * (pct / 100.0))
	lower := math.Floor(rank)
	upper := math.Ceil(rank)
	if lower == upper {
		return ordered[int(lower)]
	}
	weight := float64(rank - lower)
	lowerValue := ordered[int(lower)]
	upperValue := ordered[int(upper)]
	scaled := float64((upperValue - lowerValue) * weight)
	return float64(lowerValue + scaled)
}

// PercentileRank ports _common.py:182-189.
//
// The two counts are Python `sum(1 for ...)` over INTEGERS, so they are exact
// integer arithmetic -- pythonparity.Sum would be wrong here; it reproduces
// sum()'s FLOAT compensation, which does not apply to an int sum. The equality
// test is math.isclose with abs_tol=1e-9, which carries CPython's DEFAULT
// rel_tol of 1e-9 -- see pythonparity.IsCloseAbs for why that is not the same
// as an absolute compare.
func PercentileRank(values []float64, value float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	lower := 0
	equal := 0
	for _, candidate := range values {
		if candidate < value {
			lower++
		}
	}
	for _, candidate := range values {
		if pythonparity.IsCloseAbs(candidate, value, 1e-9) {
			equal++
		}
	}
	// The product is barriered even though both operands are exact here
	// (small integers and a halving, so no rounding can occur): the
	// repo's TestNoUnguardedFloatFMAInJobsPackages lint is deliberately
	// syntactic and does not reason about value ranges, and arguing with a
	// conservative lint costs more than the barrier does.
	halfEqual := float64(float64(equal) * 0.5)
	return float64((float64(lower)+halfEqual)/float64(len(values))) * 100.0
}

// Mean ports _common.py:192-193: `sum(values) / len(values)`.
//
// The numerator is CPython's BUILTIN sum() over floats, which since 3.12
// applies Neumaier compensated summation -- a Go `+=` loop is NOT equivalent
// and disagrees on roughly 16% of random inputs. pythonparity.Sum reproduces
// it. Mean feeds baseline_value, the anomaly baseline, and both period
// comparison values, so a last-bit difference propagates into z_score and
// trend classification.
func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	return pythonparity.Sum(values) / float64(len(values))
}

// PopulationStdev ports _common.py:196-201.
//
// The variance numerator is again a builtin sum() over floats, this time over a
// GENERATOR of squared deviations -- the compensation applies to the items
// regardless of how they are produced, so the squares are materialised and fed
// to pythonparity.Sum rather than accumulated in a loop.
func PopulationStdev(values []float64) float64 {
	if len(values) < 2 {
		return 0.0
	}
	average := Mean(values)
	deviations := make([]float64, len(values))
	for index, value := range values {
		difference := value - average
		deviations[index] = difference * difference
	}
	variance := pythonparity.Sum(deviations) / float64(len(values))
	return math.Sqrt(variance)
}

// PearsonCorrelation ports _common.py:204-216. All three sums are builtin
// sum() over floats.
func PearsonCorrelation(xs, ys []float64) float64 {
	if len(xs) != len(ys) || len(xs) < 2 {
		return 0.0
	}
	xMean := Mean(xs)
	yMean := Mean(ys)

	products := make([]float64, len(xs))
	xSquares := make([]float64, len(xs))
	ySquares := make([]float64, len(ys))
	for index := range xs {
		xDeviation := xs[index] - xMean
		yDeviation := ys[index] - yMean
		products[index] = xDeviation * yDeviation
		xSquares[index] = xDeviation * xDeviation
		ySquares[index] = yDeviation * yDeviation
	}
	numerator := pythonparity.Sum(products)
	xDenominator := math.Sqrt(pythonparity.Sum(xSquares))
	yDenominator := math.Sqrt(pythonparity.Sum(ySquares))

	// abs_tol=1e-12 here, and again CPython's default rel_tol rides along.
	if pythonparity.IsCloseAbs(xDenominator, 0.0, 1e-12) ||
		pythonparity.IsCloseAbs(yDenominator, 0.0, 1e-12) {
		return 0.0
	}
	return numerator / (xDenominator * yDenominator)
}

// FisherTwoTailedPValue ports _common.py:219-225.
//
// # THE RAW VALUE IS NOT BIT-EXACT ACROSS RUNTIMES; THE PERSISTED ONE IS
//
// It calls math.log and math.erfc. CPython delegates both to the SYSTEM libm
// while Go uses its own pure-Go implementations, so the raw results can differ
// in the last bit -- and CPython's own answer already differs between a macOS
// box and the Linux CI image for the same reason. Measured here, first case
// tried:
//
//	Go     math.Erfc/math.Log   6.354651335736282e-05
//	CPython via system libm     6.354651335736281e-05
//
// That is NOT a parity failure of the port, and it does NOT require a
// tolerance, because nothing persists the raw value. correlations.py:70 writes
// `round(p_value, 6)`, and at 6 decimals both of the above are 6.4e-05 --
// identical. The rounding granularity is roughly fifteen orders of magnitude
// coarser than libm's disagreement, so the RECORD is bit-exact even though the
// intermediate is not.
//
// Two corpus constraints keep that true, and the golden fixture must honour
// both:
//
//  1. No p_value within one ulp of a 6-decimal rounding boundary, where the
//     libm difference could flip the rounded digit.
//  2. No p_value within one ulp of 0.05. `is_significant` is computed from the
//     RAW value (correlations.py:60, before the rounding), so a case parked on
//     that threshold would be a coin flip between runtimes.
//
// Both are properties of the fixture, not of this function, and the golden
// test states them so a future corpus edit cannot quietly violate them.
func FisherTwoTailedPValue(rValue float64, sampleSize int) float64 {
	if sampleSize < 4 {
		return 1.0
	}
	boundedR := math.Max(math.Min(rValue, 0.999999), -0.999999)
	zScore := 0.5 * math.Log((1.0+boundedR)/(1.0-boundedR))
	zScore *= math.Sqrt(float64(sampleSize - 3))
	return math.Erfc(math.Abs(zScore) / math.Sqrt(2.0))
}

// AlignSeries ports _common.py:228-238: the two series reduced to their common
// days, in ascending day order.
func AlignSeries(left, right []MetricPoint) (leftValues, rightValues []float64, commonDays []time.Time) {
	leftByDay := make(map[time.Time]float64, len(left))
	for _, point := range left {
		leftByDay[point.Day] = point.Value
	}
	rightByDay := make(map[time.Time]float64, len(right))
	for _, point := range right {
		rightByDay[point.Day] = point.Value
	}

	for day := range leftByDay {
		if _, ok := rightByDay[day]; ok {
			commonDays = append(commonDays, day)
		}
	}
	sort.Slice(commonDays, func(i, j int) bool { return commonDays[i].Before(commonDays[j]) })

	leftValues = make([]float64, 0, len(commonDays))
	rightValues = make([]float64, 0, len(commonDays))
	for _, day := range commonDays {
		leftValues = append(leftValues, leftByDay[day])
		rightValues = append(rightValues, rightByDay[day])
	}
	return leftValues, rightValues, commonDays
}

// round4 and round6 are the two rounding precisions every output record uses.
// They are pythonparity.Round, NOT math.Round: CPython's round() is
// correctly-rounded half-to-even on the exact binary value, which math.Round
// (half-away-from-zero, and only to integers) is not.
func round4(value float64) float64 { return mustRound(value, 4) }
func round6(value float64) float64 { return mustRound(value, 6) }

// mustRound treats a rounding error as "leave the value alone". pythonparity
// .Round errors only on non-finite input, where CPython's round() would raise;
// a metrics row carrying an unrounded NaN is strictly better than a panicking
// nightly job, and the non-finite value itself is the signal worth keeping.
func mustRound(value float64, digits int) float64 {
	rounded, err := pythonparity.Round(value, digits)
	if err != nil {
		return value
	}
	return rounded
}
