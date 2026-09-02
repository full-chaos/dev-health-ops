package units

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// pythonFloatNumberPattern is the decimal-literal grammar PyOS_string_to_double
// accepts, once the input has already been through the ASCII transform and
// underscore removal below: an optional integer part, an optional fractional
// part (with at least one digit somewhere across the two -- "5", "5.", ".5"
// and "5.5" are all valid, "." alone is not), and an optional exponent.
//
// There is deliberately no hex branch. float(str) has none -- hex float
// literals ("0x1p-2") are float.fromhex's grammar, a completely different
// function that float() never calls. strconv.ParseFloat DOES accept "0x1p-2"
// (as 0.25), which is exactly the divergence that makes ParseFloat unusable
// here without a pre-filter: this pattern is that filter.
var pythonFloatNumberPattern = regexp.MustCompile(`^(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$`)

func isASCIIDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

// ParsePythonFloat reports the value CPython's float(s) produces, and
// ok=false where CPython raises ValueError.
//
// This ports PyFloat_FromString (Objects/floatobject.c), which runs three
// passes in order and is reproduced here as three passes in the same order:
//
//  1. _PyUnicode_TransformDecimalAndSpaceToASCII -- normalize digits/space.
//  2. _Py_string_to_number_with_underscores -- validate and strip PEP 515 '_'.
//  3. PyOS_string_to_double -- parse the resulting ASCII grammar.
//
// Doing them out of order, or folding them into one pass, changes behaviour:
// e.g. underscore validation (pass 2) must see ASCII digits already, because
// "١_٢" (Arabic-Indic digits either side of '_') is valid and the underscore
// check is a plain ASCII '0'-'9' test.
func ParsePythonFloat(s string) (float64, bool) {
	// Pass 1. Every Unicode category-Nd digit becomes its ASCII counterpart
	// (via decimalDigitValue / pythonDecimalRuns -- see constants.go for why
	// that table exists instead of unicode.IsDigit) and every Unicode space
	// becomes plain ' '. This alone is why "１２３" -> 123.0 and "\xa01.5" ->
	// 1.5: by the end of this loop they are indistinguishable from "123" and
	// " 1.5".
	//
	// unicode.IsSpace, NOT pythonparity.IsSpace. That looks like the
	// "consistent" choice and it is wrong: pythonparity.IsSpace models
	// str.isspace(), which additionally treats U+001C-U+001F as space.
	// float()'s whitespace rule does not -- measured directly:
	// "\x1c1.5" raises ValueError, "\x851.5" and "\xa01.5" both parse to 1.5.
	// Go's unicode.IsSpace matches the numeric rule (it has no U+001C-U+001F
	// entries), so it -- and only it -- belongs here.
	transformed := make([]rune, 0, len(s))
	for _, r := range s {
		if v := decimalDigitValue(r); v >= 0 {
			transformed = append(transformed, rune('0'+v))
			continue
		}
		if unicode.IsSpace(r) {
			transformed = append(transformed, ' ')
			continue
		}
		transformed = append(transformed, r)
	}

	// Pass 2. An underscore survives only when it sits strictly between two
	// (now-ASCII, by pass 1) digits. "1_000" and "1_0e1_0" are valid;
	// "_1", "1_", "1__0" and "1_.5" (the underscore touches '.', not a digit
	// on both sides) all raise ValueError. Unlike parsePythonInt's digit
	// limit, float() has NO length cap on the digit count -- only placement
	// is checked here.
	withoutUnderscores := make([]rune, 0, len(transformed))
	for i, r := range transformed {
		if r != '_' {
			withoutUnderscores = append(withoutUnderscores, r)
			continue
		}
		if i == 0 || i == len(transformed)-1 {
			return 0, false
		}
		if !isASCIIDigit(transformed[i-1]) || !isASCIIDigit(transformed[i+1]) {
			return 0, false
		}
	}

	// Pass 3. PyOS_string_to_double strips leading/trailing ASCII space --
	// the only space variant left after pass 1 -- and nothing else may
	// surround the literal: an interior space ("1 5", "1 . 5") is not part
	// of any production below and must fail, not be skipped.
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

	// inf/infinity/nan are case-insensitive and sign-carrying. Unlike inf,
	// where the sign only picks +Inf vs -Inf, NaN's sign bit is preserved
	// from the source text: float("-nan") is fff8000000000000, not
	// 7ff8000000000000. Comparing by value instead of by bits (v == math.NaN())
	// can never see this -- NaN != NaN by IEEE 754 -- which is exactly why the
	// golden test must compare math.Float64bits, never ==.
	switch asciiLower(body) {
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

	if !pythonFloatNumberPattern.MatchString(body) {
		return 0, false
	}

	signed := body
	if negative {
		signed = "-" + body
	}
	// The pattern above already enforces CPython's decimal-literal grammar,
	// so the only error ParseFloat can still return is ErrRange -- magnitude
	// too large (-> ±Inf, matching float("1e309") == inf) or too small to
	// represent even as a subnormal (-> ±0, matching float("1e-400") == 0.0,
	// sign preserved). Both are correct CPython answers, not failures, so
	// ErrRange is swallowed and the returned value is trusted: Go's decimal
	// parser and CPython's dtoa both implement correctly-rounded
	// decimal-to-binary conversion, so for the arbitrarily long digit runs in
	// the golden (a 105-digit and a 420-digit literal) the two must land on
	// the identical bit pattern -- there is only one correctly-rounded
	// answer. A non-ErrRange error here would mean the regex accepted
	// something ParseFloat doesn't, which would be a bug in the regex, not a
	// real ValueError -- treated defensively as one anyway rather than
	// panicking or returning a garbage value.
	value, err := strconv.ParseFloat(signed, 64)
	if err != nil {
		if numErr, is := err.(*strconv.NumError); is && numErr.Err == strconv.ErrRange {
			return value, true
		}
		return 0, false
	}
	return value, true
}

// asciiLower lowercases ONLY A-Z, leaving every other byte untouched.
//
// # WHY NOT strings.ToLower
//
// CPython matches the inf/infinity/nan words with ASCII-only case folding
// (PyOS_strnicmp over bytes). strings.ToLower is full Unicode, and that
// difference is reachable: it maps U+0130 (LATIN CAPITAL LETTER I WITH DOT
// ABOVE) to 'i', so "\u0130NF" folds to "inf" and matches. CPython's transform
// pass converts only category-Nd digits and Unicode spaces to ASCII, so the
// U+0130 survives, "\u0130NF" never matches "inf", and float() raises.
//
// Measured, before the fix:
//
//	float("\u0130NF")        Python: ValueError -> 0.0   Go: +Inf
//	float("iNF\u0130N\u0130TY")  Python: ValueError -> 0.0   Go: +Inf
//
// That is the exact "Go accepts what Python rejects" class this file exists to
// eliminate, reintroduced by the case-folding step inside it. The consequence is
// not cosmetic: +Inf instead of 0.0 clears the 0.2 membership threshold AND
// sorts first in the argmax, so it also takes is_dominant.
//
// strings.EqualFold is a THIRD relation -- Unicode simple folding -- and is not
// a substitute for ASCII folding in general. It accepts "fal\u017fe" as "false".
//
// For THIS keyword set, though, the two are provably equivalent, and the claim
// is measured rather than asserted. Substituting every code point at every
// position of inf/infinity/nan produced 0 disagreements between EqualFold and
// asciiLower, and the structural reason is that exactly two ASCII letters are
// reachable by a non-ASCII simple fold:
//
//	"k" <- U+212A  KELVIN SIGN
//	"s" <- U+017F  LATIN SMALL LETTER LONG S
//
// Neither appears in inf, infinity or nan, so no input can distinguish them
// here. An earlier version of this comment claimed an EqualFold divergence at
// this call site; lane-pathb-go could not reproduce it, and they were right --
// a comment asserting a defect that no input exhibits reads as measured and
// stops the next reader looking.
//
// asciiLower is still what is used, because it is the honest port of a byte-wise
// tolower and stays correct if the keyword set ever grows an "s" or a "k". Note
// the two Go primitives are wrong in DISJOINT directions -- EqualFold
// over-accepts on s/k keywords, strings.ToLower over-accepts on U+0130 -- and
// only ASCII folding is right in both.
func asciiLower(value string) string {
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
