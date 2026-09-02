package pythonparity

import (
	"testing"
	"unicode/utf8"
)

// ParseUUID's final gate is `len(hex) != 32`, which in Go counts BYTES while
// CPython's equivalent counts CHARACTERS. That is the byte-index/character
// class (lane-4752-go round 3), and the two measurements genuinely differ:
// `len("é"*32)` is 32 in Python and 64 in Go.
//
// The outcome is nevertheless identical, for a reason worth asserting rather
// than trusting: ACCEPTANCE requires 32 hexadecimal digits, every hex digit is
// ASCII, and for ASCII the two counts coincide. So the byte/character
// distinction can only affect values that are REJECTED either way — it changes
// which check rejects them, never whether.
//
// These tests pin that. If someone later relaxes the hex requirement, or
// pre-normalises non-ASCII into the candidate, the reasoning stops holding and
// these fail rather than the divergence appearing in a window somewhere.
func TestParseUUIDLengthGateIsByteSafe(t *testing.T) {
	const valid = "7b9583ee-4d24-2be7-4d09-34f815bebdd7"

	t.Run("32 multi-byte characters are rejected by both planes", func(t *testing.T) {
		// Python: len() == 32, so it passes the LENGTH gate and dies at int(hex, 16).
		// Go: len() == 64 bytes, so it dies at the length gate. Different check,
		// same verdict.
		var candidate string
		for range 32 {
			candidate += "é"
		}
		if utf8.RuneCountInString(candidate) != 32 {
			t.Fatalf("test input has %d runes, want 32", utf8.RuneCountInString(candidate))
		}
		if len(candidate) == 32 {
			t.Fatal("test input must have a byte length different from its rune count")
		}
		if _, err := ParseUUID(candidate); err == nil {
			t.Fatal("ParseUUID accepted 32 non-hex characters")
		}
	})

	t.Run("32 bytes that are fewer than 32 characters are rejected", func(t *testing.T) {
		// 30 ASCII hex digits + one 2-byte character = 32 bytes, 31 characters.
		// Python rejects on length; Go passes the length gate and rejects on hex.
		candidate := "7b9583ee4d242be74d0934f815bebdé"
		for len(candidate) < 32 {
			candidate += "a"
		}
		if len(candidate) != 32 {
			t.Fatalf("test input is %d bytes, want 32", len(candidate))
		}
		if utf8.RuneCountInString(candidate) == 32 {
			t.Fatal("test input must have fewer runes than bytes")
		}
		if _, err := ParseUUID(candidate); err == nil {
			t.Fatalf("ParseUUID accepted %q: 32 bytes but not 32 hex digits", candidate)
		}
	})

	t.Run("the accepted form is pure ASCII, where both counts agree", func(t *testing.T) {
		parsed, err := ParseUUID(valid)
		if err != nil {
			t.Fatalf("ParseUUID refused a canonical uuid: %v", err)
		}
		normalised := "7b9583ee4d242be74d0934f815bebdd7"
		if len(normalised) != utf8.RuneCountInString(normalised) {
			t.Fatal("the normalised form is not ASCII; the byte/character argument no longer holds")
		}
		if parsed.String() != valid {
			t.Fatalf("ParseUUID(%q) = %s", valid, parsed)
		}
	})
}
