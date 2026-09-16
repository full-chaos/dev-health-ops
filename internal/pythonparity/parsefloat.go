package pythonparity

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// pythonFloatGrammar is the decimal-literal grammar PyOS_string_to_double
// accepts once the input has already been through the ASCII digit/space
// transform and underscore removal below: an optional integer part, an
// optional fractional part (with at least one digit somewhere across the
// two -- "5", "5.", ".5" and "5.5" are all valid, "." alone is not), and an
// optional exponent.
//
// There is deliberately no hex branch. float(str) has none -- a hex float
// literal ("0x1p-2") is float.fromhex's grammar, a different function float()
// never calls. strconv.ParseFloat DOES accept "0x1p-2" (as 0.25), which is
// exactly the divergence that makes ParseFloat unusable here without this
// grammar as a pre-filter.
var pythonFloatGrammar = regexp.MustCompile(`^(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$`)

func isASCIIDecimalDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

// asciiFold lowercases ONLY A-Z, leaving every other byte untouched.
//
// CPython matches the inf/infinity/nan words with ASCII-only case folding
// (PyOS_strnicmp over bytes). strings.ToLower is full Unicode and maps
// U+0130 (LATIN CAPITAL LETTER I WITH DOT ABOVE) to 'i', so "İNF" would
// fold to "inf" and match -- a value CPython's float() rejects because its
// transform pass converts only category-Nd digits and Unicode spaces, never
// U+0130.
func asciiFold(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for i := 0; i < len(value); i++ {
		character := value[i]
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		builder.WriteByte(character)
	}
	return builder.String()
}

// ParseFloat reports the value CPython's float(s) produces, and ok=false
// exactly where CPython raises ValueError.
//
// This ports PyFloat_FromString (Objects/floatobject.c), which runs three
// passes in order:
//
//  1. transformDecimalAndSpaceToASCII -- the SAME fold parsePythonIntBase16
//     and ParseInt apply, because CPython funnels float(), int(str) and
//     int(str, base) through the one C function
//     (_PyUnicode_TransformDecimalAndSpaceToASCII): every category-Nd digit
//     becomes its ASCII counterpart, every int()/float()-space character
//     becomes ' '. This is why "１２３" -> 123.0 and "\xa01.5" -> 1.5.
//  2. Strip PEP 515 underscores, rejecting any that do not sit strictly
//     between two (now-ASCII) digits -- "1_000" and "1_0e1_0" are valid,
//     "_1", "1_", "1__0" and "1_.5" all raise. Unlike int()'s
//     sys.get_int_max_str_digits() cap, float() has no length limit on the
//     digit count, only on placement.
//  3. PyOS_string_to_double parses the remaining ASCII: a decimal literal,
//     or inf/infinity/nan (case-insensitive, ASCII-only fold, sign-carrying
//     -- float("-nan") is bit fff8000000000000, not 7ff8000000000000,
//     because NaN's sign survives from the source text).
//
// A magnitude too large or too small to represent (ErrRange from the
// underlying strconv.ParseFloat) is a correct CPython answer -- float("1e309")
// is inf, float("1e-400") is 0.0, sign preserved -- not a parse failure, so
// it is swallowed and the returned value trusted.
func ParseFloat(s string) (float64, bool) {
	transformed := []rune(transformDecimalAndSpaceToASCII(s))

	withoutUnderscores := make([]rune, 0, len(transformed))
	for i, r := range transformed {
		if r != '_' {
			withoutUnderscores = append(withoutUnderscores, r)
			continue
		}
		if i == 0 || i == len(transformed)-1 {
			return 0, false
		}
		if !isASCIIDecimalDigit(transformed[i-1]) || !isASCIIDecimalDigit(transformed[i+1]) {
			return 0, false
		}
	}

	// The only space variant left after pass 1 is plain ASCII ' ', which
	// PyOS_string_to_double strips from both ends; an INTERIOR space
	// ("1 5", "1 . 5") is not part of any production below and must fail,
	// not be skipped.
	trimmed := strings.TrimSpace(string(withoutUnderscores))
	if trimmed == "" {
		return 0, false
	}

	negative := false
	body := trimmed
	switch body[0] {
	case '+':
		body = body[1:]
	case '-':
		negative = true
		body = body[1:]
	}
	if body == "" {
		return 0, false
	}

	switch asciiFold(body) {
	case "inf", "infinity":
		if negative {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	case "nan":
		if negative {
			return math.Float64frombits(0xfff8000000000000), true
		}
		return math.Float64frombits(0x7ff8000000000000), true
	}

	if !pythonFloatGrammar.MatchString(body) {
		return 0, false
	}

	signed := body
	if negative {
		signed = "-" + body
	}
	value, err := strconv.ParseFloat(signed, 64)
	if err != nil {
		if numErr, is := err.(*strconv.NumError); is && numErr.Err == strconv.ErrRange {
			return value, true
		}
		// The grammar above already enforces CPython's decimal-literal
		// syntax, so the only error reachable here is ErrRange; a
		// non-ErrRange error would mean the regex accepted something
		// ParseFloat does not, which is a bug in the regex, not a genuine
		// ValueError -- handled defensively rather than trusting a garbage
		// value.
		return 0, false
	}
	return value, true
}
