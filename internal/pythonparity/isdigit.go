package pythonparity

// IsDigit reports whether r is a "digit" the way CPython's str.isdigit()
// checks one character: true for every Nd (decimal digit) codepoint --
// reusing pythonNdRuns, the same frozen table ParseInt's digit folding
// uses, rather than Go's unicode.IsDigit (which tracks a different
// Unicode version) -- plus a second, smaller set of codepoints Unicode
// classifies as "Digit" without being "Decimal": superscripts (²),
// subscripts, circled and parenthesized digits, and a few scripts' own
// digit characters. A password-strength or similar per-character check
// ported from Python's `any(c.isdigit() for c in s)` must call this, not
// unicode.IsDigit, or it rejects a password Python accepts (found live:
// "Abcdefghijk²" -- U+00B2 SUPERSCRIPT TWO -- passes Python's digit
// requirement and fails Go's under unicode.IsDigit).
func IsDigit(r rune) bool {
	if _, ok := pythonDecimalDigit(r); ok {
		return true
	}
	for _, run := range pythonDigitNotDecimalRuns {
		if r >= run[0] && r <= run[1] {
			return true
		}
	}
	return false
}
