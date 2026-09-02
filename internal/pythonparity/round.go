package pythonparity

import (
	"math"
	"math/big"
)

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
func Round(value float64, ndigits int) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	if value == 0 {
		// Preserves -0.0. big.Rat has no signed zero, so the general path
		// below could not return it.
		return value
	}

	// Guard the exponent arithmetic. Beyond these bounds the requested decimal
	// place is far outside the range where any float64 has significant digits,
	// so CPython's dtoa round-trip returns the input unchanged (large positive
	// ndigits) or zero (large negative ndigits). Computing 10**ndigits as a
	// big.Int at those magnitudes would allocate without changing the answer.
	const maxDecimalExponent = 400
	if ndigits > maxDecimalExponent {
		return value
	}
	if ndigits < -maxDecimalExponent {
		return math.Copysign(0, value)
	}

	// Step 1: round the exact binary value to `ndigits` decimal places,
	// breaking ties toward even.
	exact := new(big.Rat).SetFloat64(value)
	if exact == nil {
		return value
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
	if out == 0 {
		// A value that rounded away to zero keeps the sign of its input, as
		// CPython does; big.Rat discarded it.
		return math.Copysign(0, value)
	}
	return out
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
