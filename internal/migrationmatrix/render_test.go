package migrationmatrix

import (
	"testing"
)

// F8 (CHAOS-5581, opus-r10): shortHex's own comment claims "anything that
// is not plain hex is printed whole, so a malformed value is never
// disguised as a real one by truncation" -- but nothing exercised that
// claim, so a mutant that drops the hex charset test (truncating any long
// string) left every existing test green. A non-hex document digest is
// exactly the producer class R88's trust boundary admits: an operator
// inserting a row by hand.
func TestShortHexPrintsNonHexWhole(t *testing.T) {
	for _, c := range []struct {
		name  string
		input string
		want  string
	}{
		{"a real hex digest, truncated", "0123456789abcdef0123456789abcdef", "0123456789ab…"},
		{"exactly 12 hex characters, not truncated (len must be > 12)", "0123456789ab", "0123456789ab"},
		{"a long non-hex operation-style name, printed whole", "x' OR ''='' -- an injection-shaped operation name over 12 chars", "x' OR ''='' -- an injection-shaped operation name over 12 chars"},
		{"hex characters with one uppercase letter, printed whole", "0123456789AB0123456789ab", "0123456789AB0123456789ab"},
		{"a hyphenated UUID-shaped value, printed whole", "11111111-1111-4111-8111-111111111111", "11111111-1111-4111-8111-111111111111"},
		{"hex at both ends, non-hex in the middle, printed whole", "aaaaaaaa-XXXX-aaaaaaaa", "aaaaaaaa-XXXX-aaaaaaaa"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := shortHex(c.input); got != c.want {
				t.Fatalf("shortHex(%q) = %q, want %q", c.input, got, c.want)
			}
		})
	}
}
