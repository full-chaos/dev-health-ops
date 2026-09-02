package pythonparity

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// ErrRoundOverflow mirrors the OverflowError CPython's round() raises when the
// rounded value cannot be represented as a float.
//
// It is a real, narrow, and NON-CONTIGUOUS band, which is why this is an error
// rather than a modelled special case. Measured on CPython 3.14.7 for the
// largest float, sweeping ndigits over -320..-291, the raising values are:
//
//	-308, -307, -306, -305, -304, -299, -298, -294, -293
//
// and no others -- rounding to the nearest 10**308 lands above the maximum
// float, while rounding to the nearest 10**309 collapses to zero and is fine.
// Whether a given (value, ndigits) pair overflows is a property of the exact
// arithmetic, not of a range that could be hardcoded, so this implementation
// detects the overflow in the conversion itself rather than predicting it.
var ErrRoundOverflow = errors.New("pythonparity: rounded value too large to represent")

// ErrPrecisionMissing mirrors the ValueError CPython raises for a format spec
// with a negative precision: format(1.0, ".-1f") is "Format specifier missing
// precision", not a shortest-representation request.
var ErrPrecisionMissing = errors.New("pythonparity: format specifier missing precision")

// ErrPrecisionTooBig mirrors CPython's "ValueError: precision too big".
//
// The boundary is exact and was measured, not inferred: format() accepts a
// precision up to 2147483647 (INT_MAX) and raises at 2147483648.
//
// The ORDER matters and is also measured. CPython validates the precision
// BEFORE it takes the non-finite shortcut, so format(float("nan"),
// ".2147483648f") raises rather than returning "nan" -- even though the
// precision could not have affected the output. A mirror that checks the
// specials first answers where the reference refuses.
var ErrPrecisionTooBig = errors.New("pythonparity: precision too big")

// maxFormatPrecision is CPython's INT_MAX ceiling on a format spec's precision.
const maxFormatPrecision = 2147483647

// Round mirrors CPython's two-argument builtin `round(value, ndigits)` for
// floats.
//
// It is NOT Go's math.Round, and the difference is not a rounding-mode detail
// that washes out -- the two disagree on ordinary inputs. Measured on the
// shipped interpreter (CPython 3.14.7, arm64):
//
//	round(2.675, 2)  == 2.67    math.Round-style gives 2.68
//	round(0.125, 2)  == 0.12    ... gives 0.13
//	round(2.5, 0)    == 2.0     ... gives 3.0
//	round(0.5, 0)    == 0.0     ... gives 1.0
//	round(-0.125, 2) == -0.12   ... gives -0.13
//
// Two distinct rules produce that behaviour, and a port needs BOTH:
//
//  1. The rounding operates on the EXACT BINARY VALUE, not on the decimal
//     literal a reader sees in the source. 2.675 is really
//     2.67499999999999982236431605997495353221893310546875, so it is not a tie
//     at all and rounds DOWN. Any implementation that reasons about "2.675" as
//     a decimal gets this wrong.
//  2. A genuine tie -- 0.125 at two places, 2.5 at zero -- breaks toward EVEN,
//     not away from zero.
//
// CPython implements this by formatting the double to `ndigits` decimal places
// with David Gay's correctly-rounded dtoa and reading the result back with
// strtod (Objects/floatobject.c, `_Py_double_round`). That is a two-step
// operation and both steps round: an exact decimal rounding, then a correctly
// rounded decimal-to-binary conversion. This mirror reproduces both steps
// exactly with big.Rat rather than approximating them, so it is correct for
// negative `ndigits` (rounding to tens, hundreds) as well.
//
// Non-finite inputs are returned unchanged, matching CPython, which returns
// inf/-inf/nan from round() rather than raising.
//
// Signed zero is preserved: CPython's round(-0.0, -1) is -0.0, and a rounding
// that collapses a small negative toward zero yields -0.0, not +0.0. The sign
// of zero survives into `str()`/`repr()` output ("-0.0"), so losing it here
// would change stored text downstream.
//
// Callers in this codebase: every `round(x, N)` site feeding a
// `recommendations_daily` evidence value, where the rounded number is
// serialised into the stored `evidence_json` column.
//
// It returns an error rather than a float alone because CPython's round() has
// a failure mode -- ErrRoundOverflow -- and a mirror with no failure channel
// has to invent a value where the reference refuses. The invented value here
// would be +Inf, which is not obviously wrong at a call site and would be
// serialised into a stored column as the literal `Infinity`. A plausible wrong
// number is worse than an error, so the signature carries the error.
func Round(value float64, ndigits int) (float64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value, nil
	}
	if value == 0 {
		// Preserves -0.0. big.Rat has no signed zero, so the general path
		// below could not return it.
		return value, nil
	}

	// Short-circuit the exponent arithmetic outside the range where rounding
	// can still change a float64, so 10**ndigits is never built as a big.Int
	// at a magnitude that cannot affect the answer.
	//
	// The positive bound is MEASURED, not chosen. Sweeping ndigits 0..399 over
	// both float64 extremes plus 400 random bit patterns, the largest ndigits
	// at which round(x, n) != x is 323, witnessed by the smallest subnormal
	// (5e-324, 0x0.0000000000001p-1022). No float64 carries a digit below the
	// 323rd decimal place, so for every value and every n >= 324 the exact path
	// returns the input and this short-circuit is its exact equivalent.
	//
	// The constant was 400 before, which is why a mutant lowering it to 300
	// escaped the corpus for a round: any threshold above 324 is behaviourally
	// identical, so a test can only pin the boundary that actually bites.
	// Setting it AT the boundary makes 323 a case the corpus covers, and
	// therefore makes any lowering of this guard observable.
	const roundingReachLimit = 324
	if ndigits >= roundingReachLimit {
		return value, nil
	}
	if ndigits <= -roundingReachLimit {
		// Symmetric: rounding to the nearest 10**324 or coarser cannot leave a
		// nonzero result, because 10**324 exceeds the largest float64.
		return math.Copysign(0, value), nil
	}

	// Step 1: round the exact binary value to `ndigits` decimal places,
	// breaking ties toward even.
	exact := new(big.Rat).SetFloat64(value)
	if exact == nil {
		// Unreachable: the non-finite inputs were returned above, and
		// SetFloat64 fails only for those. Refusing rather than returning the
		// input keeps the impossible case loud instead of plausible.
		return 0, fmt.Errorf("%w: %v is not representable as an exact rational",
			ErrRoundOverflow, value)
	}
	scale := decimalScale(ndigits)
	scaled := new(big.Rat).Mul(exact, scale)
	rounded := ratRoundHalfToEven(scaled)

	// Step 2: convert the rounded decimal back to the nearest float64, the way
	// CPython's strtod does. big.Float's ToFloat64 rounds to nearest even,
	// which is the same tie rule.
	result := new(big.Rat).SetInt(rounded)
	result.Quo(result, scale)
	out, _ := new(big.Float).SetPrec(200).SetRat(result).Float64()

	// The finite input is the discriminator. big.Float reports an overflow by
	// returning an infinity, and the input cannot have been one (those
	// returned above), so an infinity here can only mean the rounded decimal
	// left the float64 range -- exactly the case CPython raises on. Detecting
	// it in the conversion rather than predicting it from ndigits is what
	// makes this correct for the non-contiguous band documented on
	// ErrRoundOverflow.
	if math.IsInf(out, 0) {
		return 0, fmt.Errorf("%w: round(%v, %d)", ErrRoundOverflow, value, ndigits)
	}
	if out == 0 {
		// A value that rounded away to zero keeps the sign of its input, as
		// CPython does; big.Rat discarded it.
		return math.Copysign(0, value), nil
	}
	return out, nil
}

// decimalScale returns 10**ndigits as an exact rational, for negative ndigits
// too.
func decimalScale(ndigits int) *big.Rat {
	if ndigits >= 0 {
		power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(ndigits)), nil)
		return new(big.Rat).SetInt(power)
	}
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-ndigits)), nil)
	return new(big.Rat).SetFrac(big.NewInt(1), power)
}

// ratRoundHalfToEven rounds an exact rational to the nearest integer, breaking
// exact halves toward the even neighbour.
//
// Written against big.Rat rather than any float type on purpose: the whole
// point of this path is that the tie test must be exact. Deciding "is this a
// tie?" in float64 would reintroduce the rounding error the function exists to
// avoid.
func ratRoundHalfToEven(value *big.Rat) *big.Int {
	numerator := value.Num()
	denominator := value.Denom()

	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))

	// Compare |remainder| * 2 against the denominator to classify the fraction
	// as below, exactly at, or above one half.
	twiceRemainder := new(big.Int).Abs(remainder)
	twiceRemainder.Lsh(twiceRemainder, 1)
	comparison := twiceRemainder.Cmp(new(big.Int).Abs(denominator))

	if comparison < 0 {
		return quotient
	}

	negative := numerator.Sign() < 0 != (denominator.Sign() < 0)
	step := big.NewInt(1)
	if negative {
		step = big.NewInt(-1)
	}

	if comparison > 0 {
		return quotient.Add(quotient, step)
	}
	// Exactly one half: keep the even neighbour.
	if quotient.Bit(0) == 0 {
		return quotient
	}
	return quotient.Add(quotient, step)
}
