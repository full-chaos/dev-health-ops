package pythonparity

import (
	"math"
	"strings"
	"testing"
)

// maxSignificantFractionPlace is the last fractional decimal place at which any
// float64 can still carry a nonzero digit.
//
// It is 2**-1074, the smallest subnormal, whose exact decimal expansion ends
// there. Measured on the shipped interpreter by expanding that value and
// finding its last nonzero fractional digit.
const maxSignificantFractionPlace = 1074

// TestFormatFixedTailIsPureZeroPadding pins the LAW the precision tail obeys,
// because the tail cannot be pinned by enumeration.
//
// Every previous round on this file was a corpus gap closed by adding values,
// and each time a mutant survived just above the new endpoint: precision 6,
// then 100, then 1100. That is not a run of bad luck, it is the shape of the
// problem -- and it differs from Round's in a way this file got wrong twice.
//
//	round(x, n):        the VALUE stops changing at n = 324, so a short-circuit
//	                    above it is a genuine equivalence and 324 is a real
//	                    boundary a corpus can sit on.
//	format(x, ".Nf"):   the value's DIGITS stop at 1074, but the STRING keeps
//	                    growing -- one '0' per unit of precision, all the way to
//	                    INT_MAX. There is no endpoint.
//
// So a corpus endpoint is arbitrary by construction: whatever the largest
// precision in the fixture is, `precision = that + 1` is unobserved, and a
// clamp there returns a string one character short of CPython's. Adding
// another value buys exactly one more round, which is what the last three
// rounds each bought.
//
// The law replaces the enumeration. For every float64 and every precision past
// 1074, CPython's output is the 1074-place rendering followed by pure zero
// padding -- verified on the interpreter across p in 1075..1399 for the
// smallest subnormal and at several magnitudes for every corpus value. Asserting
// it kills a clamp at ANY threshold rather than at the ones a fixture happens
// to sample, and it needs no interpreter at test time, so it runs in the
// ordinary unit suite rather than only under the live-oracle verb.
func TestFormatFixedTailIsPureZeroPadding(t *testing.T) {
	values := []float64{
		0.0,
		math.Copysign(0, -1),
		1.0,
		-1.0,
		0.1,
		2.675,
		5e-324,                  // smallest subnormal: the witness for 1074
		1e-323,                  // two ulps up
		2.2250738585072014e-308, // smallest normal
		1.7976931348623157e308,  // max float
		9007199254740992.0,      // 2**53
		123456789.123456789,
	}

	// Deliberately includes precisions far past any fixture entry, including
	// one past the old 1100 endpoint that the last round's mutant exploited.
	precisions := []int{1075, 1076, 1090, 1100, 1101, 1200, 1500, 2000, 5000}

	for _, value := range values {
		baseline, err := FormatFixed(value, maxSignificantFractionPlace)
		if err != nil {
			t.Fatalf("FormatFixed(%v, %d) failed: %v", value, maxSignificantFractionPlace, err)
		}

		for _, precision := range precisions {
			got, err := FormatFixed(value, precision)
			if err != nil {
				t.Fatalf("FormatFixed(%v, %d) failed: %v", value, precision, err)
			}
			want := baseline + strings.Repeat("0", precision-maxSignificantFractionPlace)
			if got != want {
				// Report the divergence position rather than two 5000-character
				// strings, which no reader can diff by eye.
				at := firstDifference(got, want)
				t.Errorf("FormatFixed(%v, %d): length %d, want %d; first difference at index %d",
					value, precision, len(got), len(want), at)
			}
		}
	}
}

// TestFormatFixedLengthGrowsWithPrecision is the cheap, direct statement of
// what a clamp breaks, kept separate because it fails for a different reason
// than the law above and a reader should be able to tell them apart.
//
// A clamp makes the output stop growing at its threshold. This notices that
// without needing to know where the threshold is.
func TestFormatFixedLengthGrowsWithPrecision(t *testing.T) {
	for _, value := range []float64{0.0, 1.0, -2.675, 5e-324, 1.7976931348623157e308} {
		previous := -1
		for _, precision := range []int{0, 1, 2, 7, 20, 101, 300, 1074, 1101, 2000} {
			text, err := FormatFixed(value, precision)
			if err != nil {
				t.Fatalf("FormatFixed(%v, %d) failed: %v", value, precision, err)
			}
			if len(text) <= previous {
				t.Errorf("FormatFixed(%v, %d) produced length %d, not greater than the previous %d: "+
					"the output stopped growing, which is what a precision clamp looks like",
					value, precision, len(text), previous)
			}
			previous = len(text)
		}
	}
}

func firstDifference(a, b string) int {
	limit := min(len(a), len(b))
	for index := 0; index < limit; index++ {
		if a[index] != b[index] {
			return index
		}
	}
	return limit
}
