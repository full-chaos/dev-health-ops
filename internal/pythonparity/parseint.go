package pythonparity

import (
	"fmt"
	"math/big"
)

// MaxIntStringDigits mirrors CPython's default sys.get_int_max_str_digits()
// (3.11+): int(str) refuses to parse a decimal string with more than this
// many DIGITS, raising ValueError -- a denial-of-service mitigation, since
// decimal-to-binary conversion of a string this long is quadratic.
// Underscores do not count toward the limit.
const MaxIntStringDigits = 4300

// ErrIntStringTooLong reports a decimal string CPython's int() refuses to
// parse because it carries more than MaxIntStringDigits digits.
var ErrIntStringTooLong = fmt.Errorf("pythonparity: more than %d digits", MaxIntStringDigits)

// ParseInt reports the value CPython's int(s) (default base 10) produces.
// The error is nil, ErrIntStringTooLong, or a plain "invalid literal"
// error -- exactly the three outcomes int(s) has (a value, an overlong
// literal, or a ValueError); callers that only care whether CPython raised
// can test err != nil.
//
// This shares its digit/space fold with parsePythonIntBase16 and ParseFloat
// (transformDecimalAndSpaceToASCII), because CPython funnels float(),
// int(str) and int(str, base) through the same C function
// (_PyUnicode_TransformDecimalAndSpaceToASCII) before grammar-checking the
// result. The grammar itself, after that fold:
//
//	space* sign? digits space*
//	sign   := "+" | "-"
//	digits := digit ("_"? digit)*
//
// Unlike base 16, there is no prefix, so an underscore may never lead: "_1"
// is invalid here where "0x_1" is valid for ParseInt(s, 16).
//
// The result is arbitrary precision (*big.Int), matching CPython -- int()
// has no width limit below the digit cap, so a Go call site that needs a
// bounded width (int64, etc.) must check big.Int.IsInt64/Uint64 or the
// equivalent itself; collapsing that here would silently pick a policy
// (error vs. saturate vs. wrap) no caller asked for.
func ParseInt(value string) (*big.Int, error) {
	runes := []rune(transformDecimalAndSpaceToASCII(value))
	position := 0

	skipSpaces := func() {
		for position < len(runes) && runes[position] == ' ' {
			position++
		}
	}
	skipSpaces()

	negative := false
	if position < len(runes) && (runes[position] == '+' || runes[position] == '-') {
		negative = runes[position] == '-'
		position++
	}

	invalid := fmt.Errorf("invalid literal for int() with base 10: %q", value)

	if position >= len(runes) || !isASCIIDecimalDigit(runes[position]) {
		return nil, invalid
	}
	digits := make([]byte, 0, len(runes))
	digits = append(digits, byte(runes[position]))
	digitCount := 1
	position++

	for position < len(runes) {
		current := runes[position]
		if current == '_' {
			// A separator must sit BETWEEN digits: neither "1__1" nor "1_"
			// parses, and (unlike base 16) there is no prefix for one to
			// follow, so a leading underscore never parses either.
			if position+1 >= len(runes) || !isASCIIDecimalDigit(runes[position+1]) {
				return nil, invalid
			}
			position++
			continue
		}
		if !isASCIIDecimalDigit(current) {
			break
		}
		digits = append(digits, byte(current))
		digitCount++
		position++
	}

	skipSpaces()
	if position != len(runes) {
		return nil, invalid
	}

	// Counted in DIGITS, not underscores or bytes: 4300 digits separated by
	// underscores is 8599 characters and still parses. This check must come
	// AFTER the syntax checks above -- CPython raises the syntax ValueError
	// first for a string that is both overlong AND malformed.
	if digitCount > MaxIntStringDigits {
		return nil, ErrIntStringTooLong
	}

	parsed, ok := new(big.Int).SetString(string(digits), 10)
	if !ok {
		// Unreachable: every byte in digits was verified ASCII '0'-'9' above.
		return nil, invalid
	}
	if negative {
		parsed.Neg(parsed)
	}
	return parsed, nil
}
