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

// expectedTailLawValueCount pins the size of the law's domain.
//
// Not a style assertion: every test in this file ranges over tailLawValues, so
// emptying or shrinking it silently disables them all while they continue to
// report ok. Raise this deliberately when adding a value -- the corpus overlap
// test will then also require the new value to be pinned.
const expectedTailLawValueCount = 12

// tailLawValues is the domain the tail law is asserted over.
//
// DELIBERATELY EXCLUDES inf and nan: the law is "baseline + zero padding", and
// precision is ignored for the non-finite values, so `format(inf, ".2000f")`
// is "inf" rather than "inf" plus padding. Adding math.Inf(1) here would fail
// confusingly rather than usefully -- the law does not apply, it is not that
// the mirror is wrong.
//
// Every value here MUST also be pinned against CPython in the golden corpus at
// precision 1074, or the law degrades from a parity claim to a
// self-consistency one. TestTailLawValuesArePinnedByTheCorpus enforces that.
var tailLawValues = []float64{
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
	values := tailLawValues

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

// TestTailLawValuesArePinnedByTheCorpus is what turns the tail law from a
// self-consistency check into a parity claim.
//
// TestFormatFixedTailIsPureZeroPadding asserts Go(v, p) == Go(v, 1074) + zeros.
// That compares Go against Go: if Go's own 1074-place rendering were wrong, it
// would still pass. It is a PARITY claim only because the golden corpus
// separately pins Go(v, 1074) against CPython for each of those values.
//
// Nothing enforced that overlap. The two lists -- this file's tailLawValues and
// the generator's corpus inputs -- were independent, and 0.1 sat in one and not
// the other, so eleven of twelve baselines were pinned and the twelfth was
// pinned by nothing. It passed anyway (CHAOS-4870, found by lane-4441).
//
// The failure mode this closes is the invisible one: adding a thirteenth value
// to tailLawValues that the corpus lacks silently unpins its baseline, the law
// test keeps passing, and nothing anywhere says the coverage shrank.
func TestTailLawValuesArePinnedByTheCorpus(t *testing.T) {
	golden := loadFloatTextGolden(t)

	pinned := make(map[uint64]bool, len(golden.Formats))
	for _, entry := range golden.Formats {
		if entry.Precision != maxSignificantFractionPlace || entry.Raises != "" {
			continue
		}
		pinned[math.Float64bits(parsePythonFloatHex(t, entry.ValueHex))] = true
	}

	for _, value := range tailLawValues {
		// Bitwise: -0.0 and +0.0 are different baselines and must be pinned
		// separately, and == would treat them as one.
		if !pinned[math.Float64bits(value)] {
			t.Errorf("tailLawValues contains %v (bits %#016x) but the corpus does not pin it "+
				"at precision %d, so the tail law's baseline for it is compared against "+
				"nothing -- add it to the generator's _SPECIALS",
				value, math.Float64bits(value), maxSignificantFractionPlace)
		}
	}

	// A corpus that lost its 1074-precision entries entirely would make the
	// loop above vacuous, so assert the oracle is non-empty rather than trust it.
	// Both sides must be non-empty, and for different reasons.
	//
	// An empty corpus side makes the loop above vacuous. An empty tailLawValues
	// makes EVERY test in this file vacuous -- TestFormatFixedTailIsPureZeroPadding
	// and TestFormatFixedLengthGrowsWithPrecision both range over it, so clearing
	// the list disables the law, the length check and this invariant at once,
	// and all three still report ok. Verified by planting it.
	//
	// That is the defect class this whole PR exists to close, reproduced inside
	// the fix: an enforcement that can be silently switched off is not an
	// enforcement. The count is asserted against the domain's actual size rather
	// than merely non-zero, so deleting values is as loud as clearing the list.
	if len(pinned) == 0 {
		t.Fatal("no corpus entries at precision 1074; this test would pass vacuously")
	}
	if len(tailLawValues) < expectedTailLawValueCount {
		t.Fatalf("tailLawValues has %d entries, expected at least %d: the law, the "+
			"length check and this invariant all range over it, so shrinking it "+
			"disables all three while every test still reports ok",
			len(tailLawValues), expectedTailLawValueCount)
	}
	t.Logf("baselines pinned at precision %d: %d; law values checked: %d",
		maxSignificantFractionPlace, len(pinned), len(tailLawValues))
}
