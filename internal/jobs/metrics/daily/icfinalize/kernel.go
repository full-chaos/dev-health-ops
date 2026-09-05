// Package icfinalize ports compute_ic.py's finalize-scope kernel
// (CHAOS-4290). Every function here mirrors ONE Python function; the mapping
// is stated per symbol so a reader can check the port against its reference
// without inferring which one it is.
package icfinalize

import "math"

// PercentileRank mirrors compute_ic.py's _percentile_rank.
//
// It computes the MEAN (midpoint) rank, NOT the weak rank. That distinction is
// the single most load-bearing fact in this package, because the Python's own
// comment contradicts its code: the comment says `Kind='weak': fraction of
// scores <= x`, which would be (count_less + count_equal)/n, while the code
// computes (count_less + 0.5*count_equal)/n. Team-lead's ruling: behaviour is
// the contract, the comment is the defect (filed separately, Python untouched).
//
// The two agree only when nothing ties -- and ties dominate here. churn,
// delivery and wip are all 0 for a low-activity identity, so every quiet
// member of a team ties with every other. A port written from the comment
// would be wrong for precisely the common case.
//
// Empty input returns 0.5, not 0 and not NaN. A one-member team therefore also
// ranks 0.5 on every axis, by construction -- the same value an empty vector
// gives, which is worth knowing when reading the output.
func PercentileRank(values []float64, value float64) float64 {
	if len(values) == 0 {
		return 0.5
	}
	var countLess, countEqual int
	for _, v := range values {
		switch {
		case v < value:
			countLess++
		case v == value:
			countEqual++
		}
	}
	// FMA BARRIER (CHAOS-4818 class). `countLess + 0.5*countEqual` is exactly
	// the z + x*y shape Go fuses into a single FMA on arm64, rounding ONCE,
	// where CPython rounds the multiply and the add separately. Production is
	// arm64. The explicit float64() conversion around the PRODUCT at its
	// definition is the spelling this package already uses --
	// testops_risk_native_clickhouse.go:930 writes
	// `weights[i] = 1.0 + float64(float64(i)*0.5)` for the same reason.
	product := float64(0.5 * float64(countEqual))
	return (float64(countLess) + product) / float64(len(values))
}

// LandscapeAxes mirrors the three map coordinates compute_ic.py builds at
// :229-238. The asymmetry is deliberate and is the reference's, not a
// simplification: log1p is applied to churn and to cycle, but wip is used RAW.
// delivery is the y axis on all three.
//
// Each log1p result is bound to a variable before use so it cannot be fused
// into a following multiply-add -- same barrier discipline as PercentileRank,
// applied where the result feeds arithmetic rather than where it is produced.
func LandscapeAxes(churn, delivery, cycle, wip float64) (churnXY, cycleXY, wipXY [2]float64) {
	x1 := math.Log1p(churn)
	x2 := math.Log1p(cycle)
	return [2]float64{x1, delivery}, [2]float64{x2, delivery}, [2]float64{wip, delivery}
}
