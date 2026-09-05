package pythonparity

import "math"

// DefaultRelTol is CPython's default rel_tol for math.isclose.
//
// IT APPLIES EVEN WHEN THE CALLER PASSES ONLY abs_tol. That is the whole
// reason this file exists -- see IsClose.
const DefaultRelTol = 1e-09

// DefaultAbsTol is CPython's default abs_tol for math.isclose.
const DefaultAbsTol = 0.0

// IsClose reproduces CPython's math.isclose.
//
// # IT IS NOT AN ABSOLUTE-TOLERANCE COMPARE
//
// The signature is isclose(a, b, *, rel_tol=1e-09, abs_tol=0.0) and the test is
//
//	|a-b| <= max(rel_tol * max(|a|, |b|), abs_tol)
//
// so rel_tol is 1e-09 by DEFAULT and stays active when a caller passes only
// abs_tol. The obvious Go transliteration of `math.isclose(x, y, abs_tol=1e-9)`
// --
//
//	math.Abs(x-y) <= 1e-9
//
// -- is therefore WRONG whenever both operands are large:
//
//	isclose(1000.0, 1000.0000005, abs_tol=1e-9)  -> True   (rel term 1.0e-06)
//	math.Abs(1000.0-1000.0000005) <= 1e-9        -> false
//
// Measured against the live interpreter, not argued.
//
// # WHY A BENIGN CORPUS HIDES IT
//
// Most call sites in the benchmarking family compare against ZERO
// (`isclose(x, 0.0, abs_tol=t)`), where the relative term is
// rel_tol*max(|x|,0) and collapses to |x| <= t for any x small enough to be
// interesting. Those sites agree with the naive form, so a fixture built only
// from them proves nothing about the one site that differs.
//
// # WHERE IT BITES IN THIS PORT
//
// anomalies.py:69, `isclose(current_point.value, baseline_value, abs_tol=1e-9)`
// -- both operands are metric values and routinely far greater than 1. It
// decides whether a zero-variance history yields z_score 0.0 or 3.0, which
// selects the anomaly's severity ("info" vs "critical") and whether the row is
// emitted at all. A last-bit disagreement here is a visible, alerting-level
// output change, not a rounding curiosity.
//
// # EDGE CASES, ALL CPython's
//
// Identical values are close even at infinity (isclose(inf, inf) is True);
// mixed-sign infinities are never close; NaN is never close to anything,
// including itself; and a negative tolerance is an error in CPython, which is
// reported here as false rather than by panicking, since every caller in this
// repo passes a compile-time constant.
func IsClose(a, b, relTol, absTol float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) || math.IsNaN(relTol) || math.IsNaN(absTol) {
		return false
	}
	if relTol < 0 || absTol < 0 {
		// CPython raises ValueError. No caller in this repo can reach it, and
		// returning false keeps this usable from a pure comparison site.
		return false
	}
	// CPython short-circuits on exact equality FIRST, which is what makes
	// isclose(inf, inf) true -- the subtraction below would be inf-inf = NaN.
	if a == b {
		return true
	}
	if math.IsInf(a, 0) || math.IsInf(b, 0) {
		// Not equal, and at least one is infinite: the difference is infinite.
		return false
	}
	difference := math.Abs(b - a)
	return difference <= math.Abs(relTol*b) ||
		difference <= math.Abs(relTol*a) ||
		difference <= absTol
}

// IsCloseAbs is the shorthand for CPython's `math.isclose(a, b, abs_tol=t)` --
// note that it still carries the DEFAULT rel_tol, because CPython does. Use
// this at every ported `math.isclose(..., abs_tol=...)` call site so the
// defaulting is impossible to forget.
func IsCloseAbs(a, b, absTol float64) bool {
	return IsClose(a, b, DefaultRelTol, absTol)
}
