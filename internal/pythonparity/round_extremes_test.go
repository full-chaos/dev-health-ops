package pythonparity

import (
	"math"
	"testing"
)

// TestRoundAtExtremeNDigitsMatchesMeasuredPython pins the short-circuit in
// Round that the golden corpus cannot reach.
//
// The corpus sweeps ndigits in [-2, 10], so the `maxDecimalExponent = 400`
// branch is invisible to it -- the branch was originally justified by a
// comment alone, which is exactly the shape ("prose standing in for a
// measurement") that a port review is supposed to reject. These expectations
// are the measured output of the shipped interpreter (CPython 3.14.7, arm64):
//
//	round(1.2345, 401)   == 1.2345      round(1.2345, -401)   == 0.0
//	round(-1.2345, 401)  == -1.2345     round(-1.2345, -401)  == -0.0
//	round(5e-324, 1000)  == 5e-324      round(5e-324, -400)   == 0.0
//	round(1.797...e308, 400) == itself  round(1.797...e308, -1000) == 0.0
//
// The negative side is the one worth pinning: CPython returns a SIGNED zero
// there, so a value that rounds away to nothing keeps its sign. A short-circuit
// returning a bare 0 would be wrong for every negative input, and wrong in a
// way that only surfaces later as "-0.0" versus "0.0" in stored text.
func TestRoundAtExtremeNDigitsMatchesMeasuredPython(t *testing.T) {
	maxFloat := 1.7976931348623157e308
	smallestSubnormal := 5e-324

	cases := []struct {
		name    string
		value   float64
		ndigits int
		want    float64
	}{
		{"positive value, ndigits just past the short-circuit", 1.2345, 401, 1.2345},
		{"positive value, ndigits at the short-circuit", 1.2345, 400, 1.2345},
		{"positive value, ndigits far past it", 1.2345, 1000, 1.2345},
		{"negative value keeps its digits", -1.2345, 401, -1.2345},
		{"smallest subnormal survives", smallestSubnormal, 1000, smallestSubnormal},
		{"max float survives", maxFloat, 400, maxFloat},

		{"positive value rounds away to +0", 1.2345, -401, 0.0},
		{"positive value rounds away at the boundary", 1.2345, -400, 0.0},
		{"negative value rounds away to -0", -1.2345, -401, math.Copysign(0, -1)},
		{"negative value rounds away far past it", -1.2345, -1000, math.Copysign(0, -1)},
		{"subnormal rounds away to +0", smallestSubnormal, -400, 0.0},
		{"max float rounds away to +0", maxFloat, -1000, 0.0},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := Round(testCase.value, testCase.ndigits)
			if err != nil {
				t.Fatalf("Round(%v, %d) failed: %v", testCase.value, testCase.ndigits, err)
			}
			// Bitwise, so the signed zeros above are actually asserted --
			// under == every one of them would pass against the wrong sign.
			if math.Float64bits(got) != math.Float64bits(testCase.want) {
				t.Fatalf("Round(%v, %d) = %v (bits %#016x); python = %v (bits %#016x)",
					testCase.value, testCase.ndigits, got, math.Float64bits(got),
					testCase.want, math.Float64bits(testCase.want))
			}
		})
	}
}

// TestRoundBandsBelowTheShortCircuitUseTheExactPath guards against "widening"
// the short-circuit on the mistaken belief that the bands next to it are
// equivalent.
//
// They are not: at ndigits = -3 an ordinary value still rounds to a real
// non-zero multiple of 1000, and at ndigits = 300 a subnormal still carries
// digits. Both go through the exact big.Rat path. A future change that raised
// the short-circuit toward these bands would silently start returning the
// input (or zero) where CPython returns a rounded value.
func TestRoundBandsBelowTheShortCircuitUseTheExactPath(t *testing.T) {
	if got, _ := Round(12345.6789, -3); got != 12000.0 {
		want := 12000.0
		t.Errorf("Round(12345.6789, -3) = %v; python = %v", got, want)
	}
	if got, _ := Round(1500.0, -3); got != 2000.0 {
		want := 2000.0
		// A tie at the thousands place: 1.5 -> 2 keeps the even neighbour.
		t.Errorf("Round(1500.0, -3) = %v; python = %v", got, want)
	}
	if got, _ := Round(2500.0, -3); got != 2000.0 {
		want := 2000.0
		t.Errorf("Round(2500.0, -3) = %v; python = %v (ties toward even)", got, want)
	}
}
