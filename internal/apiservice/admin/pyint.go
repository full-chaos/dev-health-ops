package admin

import "strings"

// parsePyInt is Python's int(str) for a plain decimal literal: an optional
// leading whitespace/sign, then digits, with a single underscore allowed
// strictly BETWEEN two digits as a separator (PEP 515) -- never leading,
// trailing, doubled, or adjacent to the sign. ok is false for anything
// int(str) itself raises ValueError for (empirically verified: "_10",
// "10_", "1__0" all raise; "1_0", "-1_0", "01_0" all parse).
func parsePyInt(raw string) (int, bool) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return 0, false
	}
	negative := false
	if text[0] == '+' || text[0] == '-' {
		negative = text[0] == '-'
		text = text[1:]
	}
	if text == "" {
		return 0, false
	}
	var digits strings.Builder
	lastWasDigit := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c >= '0' && c <= '9':
			digits.WriteByte(c)
			lastWasDigit = true
		case c == '_':
			// Only valid strictly between two digits: the char before must
			// have been a digit, and the char after must exist and be a
			// digit too.
			if !lastWasDigit || i+1 >= len(text) || text[i+1] < '0' || text[i+1] > '9' {
				return 0, false
			}
			lastWasDigit = false
		default:
			return 0, false
		}
	}
	if digits.Len() == 0 {
		return 0, false
	}
	value := 0
	for _, c := range digits.String() {
		value = value*10 + int(c-'0')
	}
	if negative {
		value = -value
	}
	return value, true
}
