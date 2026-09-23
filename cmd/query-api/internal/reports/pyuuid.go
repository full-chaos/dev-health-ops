package reports

import (
	"fmt"
	"math/big"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ParseReportID ports uuid.UUID(text) as the Python resolvers call it on a
// report id: a malformed value is an error, and every value the constructor
// accepts names a row. The constructor removes every "urn:" and "uuid:", trims
// braces from both ends, removes every hyphen, requires exactly 32 characters,
// and reads them with int(text, 16) -- which itself accepts surrounding
// whitespace, one sign, a 0x prefix, single underscores between digits and any
// Unicode decimal digit. The value must fit 128 bits.
func ParseReportID(text string) (string, error) {
	h := strings.ReplaceAll(text, "urn:", "")
	h = strings.ReplaceAll(h, "uuid:", "")
	h = strings.Trim(h, "{}")
	h = strings.ReplaceAll(h, "-", "")
	if utf8.RuneCountInString(h) != 32 {
		return "", fmt.Errorf("badly formed hexadecimal UUID string")
	}
	value, ok := pyIntBase16(h)
	if !ok {
		return "", fmt.Errorf("invalid literal for int() with base 16: %s", pythonparity.StrRepr(h))
	}
	// 32 characters hold at most 128 bits and the hyphen removal leaves no
	// sign to read, so the value is always in the 128-bit range.
	raw := fmt.Sprintf("%032x", value)
	return raw[0:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32], nil
}

// digitValue is the value of a Unicode decimal digit (Nd), or -1.
func digitValue(r rune) int {
	if !unicode.IsDigit(r) {
		return -1
	}
	start := r
	for unicode.IsDigit(start - 1) {
		start--
	}
	return int(r-start) % 10
}

// pyIntBase16 is int(text, 16): ok is false where Python raises ValueError.
func pyIntBase16(text string) (*big.Int, bool) {
	s := strings.TrimFunc(text, unicode.IsSpace)
	// A hyphen never reaches this point (ParseReportID removed them all), so
	// the only sign int() can meet is "+".
	s = strings.TrimPrefix(s, "+")
	prefixed := false
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		s = s[2:]
		prefixed = true
	}
	if s == "" || strings.HasSuffix(s, "_") || strings.Contains(s, "__") || (strings.HasPrefix(s, "_") && !prefixed) {
		return nil, false
	}
	value := new(big.Int)
	sixteen := big.NewInt(16)
	for _, r := range s {
		if r == '_' {
			continue
		}
		d := digitValue(r)
		switch {
		case d >= 0:
		case r >= 'a' && r <= 'f':
			d = int(r-'a') + 10
		case r >= 'A' && r <= 'F':
			d = int(r-'A') + 10
		default:
			return nil, false
		}
		value.Mul(value, sixteen)
		value.Add(value, big.NewInt(int64(d)))
	}
	return value, true
}
