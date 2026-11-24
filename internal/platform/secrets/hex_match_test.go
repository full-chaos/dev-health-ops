package secrets

import "testing"

var hexDSN = "postgres://acct:ab%2Fxy9@db.example/app"

// An escaped spelling matches the original text; only the hex digits after a
// '%' of the spelling are case-insensitive. Text before the spelling is never
// changed, so a literal "%ab" in front of it does not hide it.
func TestEscapedSpellingMatchesAfterALiteralPercentSequence(t *testing.T) {
	b := NewBoundary(hexDSN)
	for text, want := range map[string]string{
		"authentication failed for %ab%2Fxy9":  "authentication failed for %[REDACTED]",
		"authentication failed for %0ab%2fxy9": "authentication failed for %0[REDACTED]",
		"authentication failed for %ab%2fxy9":  "authentication failed for %[REDACTED]",
		"authentication failed for ab%2fxy9":   "authentication failed for [REDACTED]",
		"authentication failed for ab%2Fxy9":   "authentication failed for [REDACTED]",
	} {
		if got := b.RedactText(text); got != want {
			t.Errorf("%q: got %q, want %q", text, got, want)
		}
	}
}

// Only the hex digits of a %XX compare case-insensitively: a letter that is not
// one differs, so "AB%2Fxy9" is not the spelling "ab%2Fxy9".
func TestOnlyTheHexDigitsOfAnEscapeIgnoreCase(t *testing.T) {
	b := NewBoundary(hexDSN)
	for _, text := range []string{"failed for AB%2Fxy9", "failed for ab%2FXY9", "failed for aB%2fxy9"} {
		if got := b.RedactText(text); got != text {
			t.Errorf("%q: got %q, want the text unchanged", text, got)
		}
	}
}

// A literal %ab or %0a with no credential in the text is left alone.
func TestLiteralPercentSequencesWithoutACredentialStayIdentical(t *testing.T) {
	b := NewBoundary(hexDSN)
	for _, text := range []string{"x %ab y %0a z", "%ab", "%0a%0A%aB", "100%ab and %2f%2F", "ab/xy"} {
		if got := b.RedactText(text); got != text {
			t.Errorf("%q: got %q, want the text unchanged", text, got)
		}
	}
}
