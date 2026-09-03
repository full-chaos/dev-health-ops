package remaining

import (
	"math"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Gini mirrors `_gini` in src/dev_health_ops/recommendations/loader.py:69.
//
// The result is compared against REVIEWER_GINI_THRESHOLD (0.6) to decide
// whether the review-concentration recommendation fires, so this is a
// CATEGORICAL output: a last-bit difference is a stored row that either exists
// or does not.
//
// # There is a second gini in the reference. It is NOT this one.
//
// src/dev_health_ops/recommendations/rules/review_concentration.py:42 defines
// another `gini` that behaves DIFFERENTLY: no positive filter, 0.0 for empty
// input, and zeros and negatives counted in n/total/sorted_vals. For
// [5.0, 0.0, 0.0] this one returns nil (one positive) and that one returns a
// number over n=3.
//
// It is dead: evaluate_review_concentration reads snapshot.reviewer_gini
// (review_concentration.py:71,75) and never calls its own module-level
// function; the only callers are that module's unit tests. This port mirrors
// the LIVE one and deliberately does not port the dead one. Stated here rather
// than only in the PR body, because "there is a second implementation and we
// chose" is exactly what a later reader needs and cannot infer from absence.
//
// # Float parity
//
// Both sums are CPython `sum()`, which is Neumaier-compensated since 3.12, so
// they use pythonparity.Sum rather than a `+=` loop. Measured on this family's
// _linear_slope, the sibling function: 40.87% of results differ in bits between
// compensated and naive accumulation, and 0.0020% cross a firing threshold
// (receipt: .remember/lanes/lane-3092-metrics-remaining/
// compute-receipt-slope-sum-20260902.md).
//
// The `(i+1) * v` product is wrapped in an explicit float64() conversion. Go
// may fuse `x*y + z` into one FMA on arm64 while CPython always rounds twice,
// and Go's spec forbids fusing across an explicit conversion (CHAOS-4818).
// Here the product feeds Sum rather than a bare `+`, but the conversion is
// kept because the alternative is a reader having to prove no fusion is
// possible, and that proof would have to be redone after every edit.
func Gini(values []float64) (float64, bool) {
	positives := make([]float64, 0, len(values))
	for _, value := range values {
		// `v > 0` is false for NaN, for -0.0 and for +0.0, matching Python's
		// filter exactly. A `>= 0` or a `!= 0` here would admit zeros the
		// reference excludes and change n.
		if value > 0 {
			positives = append(positives, value)
		}
	}
	if len(positives) < 2 {
		return 0, false
	}

	total := pythonparity.Sum(positives)
	if total == 0.0 {
		return 0, true
	}

	count := len(positives)
	sorted := make([]float64, len(positives))
	copy(sorted, positives)
	// Python's sorted() is stable, but every element here is a distinct
	// position in a sum, so order among equals cannot change the result. What
	// matters is ascending order, which sort.Float64s gives. NaN cannot reach
	// this slice: the `> 0` filter above excludes it.
	sort.Float64s(sorted)

	weighted := make([]float64, len(sorted))
	for index, value := range sorted {
		weighted[index] = float64(float64(index+1) * value)
	}
	cumulative := pythonparity.Sum(weighted)

	countAsFloat := float64(count)
	return (2.0*cumulative)/(countAsFloat*total) - (countAsFloat+1.0)/countAsFloat, true
}

// LinearSlope mirrors `_linear_slope`, which the reference defines TWICE with
// byte-identical bodies: rules/saturation.py:44 and
// rules/sustainability_risk.py:44. Diffed; they are the same function, so one
// mirror is faithful to both. If they ever diverge, this comment is the place
// that stops a reader assuming they did not.
//
// Its quotient is compared against WIP_RISING_SLOPE_THRESHOLD (0.1) and
// CYCLE_TIME_RISING_SLOPE_THRESHOLD (0.1). Categorical again, and this is the
// function whose measured flip rate produced the receipt cited above: with
// naive accumulation, [0.1, 0.0, 1.0, 0.8, 0.2] gives 0.09999999999999999 and
// does not fire, where CPython gives exactly 0.1 and does.
//
// Three sums, two of them over products; all three go through Sum, and every
// product carries an explicit float64() for the FMA reason above.
func LinearSlope(values []float64) float64 {
	count := len(values)
	if count < 2 {
		return 0.0
	}

	// (n-1)/2.0 in Python is true division on ints, so this is exact for every
	// n a slice length can hold.
	meanIndex := float64(count-1) / 2.0
	meanValue := pythonparity.Sum(values) / float64(count)

	numeratorTerms := make([]float64, count)
	denominatorTerms := make([]float64, count)
	for index, value := range values {
		offset := float64(index) - meanIndex
		numeratorTerms[index] = float64(offset * (value - meanValue))
		denominatorTerms[index] = float64(offset * offset)
	}

	denominator := pythonparity.Sum(denominatorTerms)
	if denominator == 0 {
		// Python's `num / den if den else 0.0` — a falsy check, so it catches
		// -0.0 as well as +0.0. Go's `== 0` is true for both, matching it.
		return 0.0
	}
	return pythonparity.Sum(numeratorTerms) / denominator
}

// SafeFloat mirrors `_safe_float` (loader.py:83): coerce to float, return
// "absent" for None and for NaN — but NOT for infinities.
//
// The asymmetry is the point and it is load-bearing downstream. ±Inf passes
// through into snapshot fields and reaches evidence_json, where CPython's
// json.dumps (allow_nan defaults True) writes the bare token `Infinity`, which
// is not valid JSON and which Go's encoding/json refuses to decode. Anything
// treating Inf as absent here would silently diverge from the reference on a
// value the reference deliberately keeps.
func SafeFloat(value float64, present bool) (float64, bool) {
	if !present {
		return 0, false
	}
	if math.IsNaN(value) {
		return 0, false
	}
	return value, true
}
