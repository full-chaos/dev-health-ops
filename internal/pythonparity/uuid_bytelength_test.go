package pythonparity

import (
	"testing"
	"unicode/utf8"
)

// ParseUUID's gate counts CHARACTERS, matching CPython's `len()`. This file
// pins the inputs where that differs from counting bytes.
//
// # A correction, kept visible on purpose
//
// This file used to argue the opposite: that counting bytes was SAFE here,
// because "acceptance requires 32 hexadecimal digits, every hex digit is
// ASCII, and for ASCII the two counts coincide". The premise was false. The
// step behind the gate is `int(hex, 16)`, not a hex decode, and int() folds
// Unicode decimal digits to ASCII — so acceptance does NOT require ASCII, and
// `uuid.UUID("１" * 32)` is a valid UUID with 32 characters and 96 bytes.
//
// A byte-counting gate refused it. The argument was wrong at its first step,
// and it read as rigorous, which is why it survived four review rounds and one
// planted-defect pass before a codex round constructed the counterexample.
//
// The lesson generalises past this file: an argument that a divergence "cannot
// affect the outcome" is a claim about the WHOLE pipeline behind the check, not
// about the check. It is only as good as one's model of the step downstream —
// and that is exactly the step nobody re-measures.
func TestParseUUIDLengthGateCountsCharacters(t *testing.T) {
	const valid = "7b9583ee-4d24-2be7-4d09-34f815bebdd7"

	t.Run("32 multi-byte characters are rejected by both planes", func(t *testing.T) {
		// Both planes pass the LENGTH gate (32 characters) and die at
		// int(hex, 16): "é" is not Nd, so it does not fold to a digit. A
		// byte-counting gate would reject this earlier, for the wrong reason.
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
		// Both planes now reject on LENGTH. A byte-counting gate would let this
		// through to the grammar instead — the mirror of the case above.
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

	t.Run("a canonical value still parses", func(t *testing.T) {
		parsed, err := ParseUUID(valid)
		if err != nil {
			t.Fatalf("ParseUUID refused a canonical uuid: %v", err)
		}
		if parsed.String() != valid {
			t.Fatalf("ParseUUID(%q) = %s", valid, parsed)
		}
	})
}
