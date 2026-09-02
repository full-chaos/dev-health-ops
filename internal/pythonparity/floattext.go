package pythonparity

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Repr mirrors CPython's `repr(x)` / `str(x)` for a float. The two builtins
// have produced identical output for floats since Python 3.1, so one mirror
// serves both.
//
// Why this cannot be `fmt.Sprint` or `strconv.FormatFloat(v, 'g', -1, 64)`:
// both render an integral float WITHOUT a fractional part, and CPython always
// keeps one. Measured on the shipped interpreter (3.14.7) against Go 1.27:
//
//	value    CPython str()   Go fmt.Sprint / FormatFloat('g',-1)
//	0.0      "0.0"           "0"
//	-0.0     "-0.0"          "-0" (and "0" for the constant-folded literal)
//	24.0     "24.0"          "24"
//	1e16     "1e+16"         "1e+16"
//
// This is not cosmetic in this codebase. These strings are interpolated into
// the `rationale` text of `recommendations_daily` rows -- e.g.
// `f"(threshold: {THROUGHPUT_FLAT_DELTA_THRESHOLD})"` with the constant 0.0
// renders "(threshold: 0.0)". A Go port using fmt would store
// "(threshold: 0)", a different byte string in a persisted column, for every
// row of the family.
//
// Both implementations use the SHORTEST representation that round-trips, so
// the digits themselves agree; the divergences are confined to the
// integral-value suffix and to how each spells the exponent and the specials.
func Repr(value float64) string {
	switch {
	case math.IsNaN(value):
		return "nan"
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	}

	// Take the SHORTEST round-tripping digits, the same set CPython's
	// repr-mode dtoa produces, and choose the notation ourselves.
	//
	// Go's 'g' verb cannot be used to make that choice. Its rule is relative
	// to the number of significant digits (exponent form when
	// exp < -4 || exp >= numberOfDigits), so 1e10 -- one significant digit --
	// comes out as "1e+10". CPython's rule is absolute: fixed notation for
	// every value whose decimal point sits within a fixed window, so 1e10 is
	// "10000000000.0". An earlier version of this function asserted the two
	// windows were the same; the generated corpus disagreed on 60 values
	// between 1e8 and 7e15, which is why the notation decision is now spelled
	// out here rather than delegated.
	digits, decimalPointPosition, negative := shortestDecimalDigits(value)

	// CPython's format_float_short with format code 'r': use exponent notation
	// when the decimal point falls at or left of position -3, or right of
	// position 16 (Python/pystrtod.c). Everything else is fixed notation.
	if decimalPointPosition <= -4 || decimalPointPosition > reprFixedNotationLimit {
		return buildExponentForm(digits, decimalPointPosition, negative)
	}
	return buildFixedForm(digits, decimalPointPosition, negative)
}

// reprFixedNotationLimit is the highest decimal-point position CPython still
// renders in fixed notation for repr(). At 16 the value 1e15 prints as
// "1000000000000000.0"; at 17 the value 1e16 prints as "1e+16".
const reprFixedNotationLimit = 16

// shortestDecimalDigits returns the shortest round-tripping decimal digits of
// value, the position of the decimal point relative to the start of those
// digits, and whether the value is negative (including negative zero).
//
// A decimalPointPosition of 1 means the point sits after the first digit, so
// digits "5" with position 1 is 5.0 and with position -1 is 0.005.
func shortestDecimalDigits(value float64) (digits string, decimalPointPosition int, negative bool) {
	negative = math.Signbit(value)

	// 'e' with precision -1 gives the shortest round-tripping mantissa, which
	// is the same digit string CPython works from.
	text := strconv.FormatFloat(math.Abs(value), 'e', -1, 64)

	exponentIndex := strings.IndexByte(text, 'e')
	mantissa := text[:exponentIndex]
	exponent, err := strconv.Atoi(text[exponentIndex+1:])
	if err != nil {
		// Unreachable for a finite float64; Go always emits an exponent here.
		// Returning the mantissa unchanged is the least-surprising fallback
		// and cannot silently produce a plausible wrong number.
		return strings.Replace(mantissa, ".", "", 1), 1, negative
	}

	digits = strings.Replace(mantissa, ".", "", 1)
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		digits = "0"
	}
	return digits, exponent + 1, negative
}

// buildFixedForm renders digits in CPython's fixed notation, always with at
// least one digit after the decimal point.
func buildFixedForm(digits string, decimalPointPosition int, negative bool) string {
	var builder strings.Builder
	if negative {
		builder.WriteByte('-')
	}

	switch {
	case decimalPointPosition <= 0:
		// 0.000ddd -- the point precedes every significant digit.
		builder.WriteString("0.")
		builder.WriteString(strings.Repeat("0", -decimalPointPosition))
		builder.WriteString(digits)
	case decimalPointPosition >= len(digits):
		// Integral: pad out to the point, then the ".0" CPython always keeps.
		builder.WriteString(digits)
		builder.WriteString(strings.Repeat("0", decimalPointPosition-len(digits)))
		builder.WriteString(".0")
	default:
		builder.WriteString(digits[:decimalPointPosition])
		builder.WriteByte('.')
		builder.WriteString(digits[decimalPointPosition:])
	}
	return builder.String()
}

// buildExponentForm renders digits in CPython's exponent notation: a bare
// leading digit (no ".0" padding of the mantissa), then a signed exponent of
// at least two digits.
func buildExponentForm(digits string, decimalPointPosition int, negative bool) string {
	var builder strings.Builder
	if negative {
		builder.WriteByte('-')
	}
	builder.WriteString(digits[:1])
	if len(digits) > 1 {
		builder.WriteByte('.')
		builder.WriteString(digits[1:])
	}

	exponent := decimalPointPosition - 1
	builder.WriteByte('e')
	if exponent < 0 {
		builder.WriteByte('-')
		exponent = -exponent
	} else {
		builder.WriteByte('+')
	}
	exponentText := strconv.Itoa(exponent)
	if len(exponentText) < 2 {
		builder.WriteByte('0')
	}
	builder.WriteString(exponentText)
	return builder.String()
}

// FormatFixed mirrors CPython's fixed-precision format spec, `format(x, ".Nf")`
// -- the `:.3f`, `:.1f` and `:.2f` interpolations that build the stored
// `rationale` strings of the recommendations family.
//
// CPython and Go agree on the digits here: both round the exact binary value
// to the requested number of decimal places, breaking ties toward even. They
// disagree only on how they spell the non-finite specials, and Go's names for
// those ("+Inf", "NaN") would land verbatim in a persisted column.
//
//	value   CPython format(x,'.3f')   Go strconv.FormatFloat(x,'f',3,64)
//	+inf    "inf"                     "+Inf"
//	-inf    "-inf"                    "-Inf"
//	nan     "nan"                     "NaN"
//
// The specials are reachable on this path rather than theoretical: the
// reference's `_safe_float` (recommendations/loader.py) strips NaN but PASSES
// ±Inf through, so an infinite ratio can reach a rationale string.
//
// A NEGATIVE precision is refused rather than honoured. Go treats precision -1
// as "shortest representation that round-trips", so `FormatFixed(1.0, -1)`
// would return "1" -- a perfectly plausible string. CPython has no such mode
// in this spec: `format(1.0, ".-1f")` raises
// `ValueError: Format specifier missing precision`. Silently answering where
// the reference refuses is the divergence that is hardest to notice, because
// the output looks like a successful format rather than like a bug.
func FormatFixed(value float64, precision int) (string, error) {
	if precision < 0 {
		return "", fmt.Errorf("%w: .%df", ErrPrecisionMissing, precision)
	}
	switch {
	case math.IsNaN(value):
		return "nan", nil
	case math.IsInf(value, 1):
		return "inf", nil
	case math.IsInf(value, -1):
		return "-inf", nil
	}
	return strconv.FormatFloat(value, 'f', precision, 64), nil
}
