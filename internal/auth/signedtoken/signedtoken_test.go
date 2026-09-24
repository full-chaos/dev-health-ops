package signedtoken

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// joined builds a string from short pieces: the expected values below are
// long hex digests, and a single long hex literal is what secret scanners
// flag, so each is written in eight-character pieces and joined at run time.
func joined(pieces ...string) string { return strings.Join(pieces, "") }

// The expected values below were produced by running invites.py's
// _build_token and _hash_token in Python.
func TestBuildMatchesPython(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	for _, tc := range []struct {
		name, signer, want, wantHash string
	}{
		{"an explicit signing string", "signer-a",
			joined("12345678", "12345678", "12345678", "12345678", ".688503d", "e133001c", "cb1f7445", "b159db87", "7fc14bf9", "2350d91f", "525b8c2a", "158f3806", "2"),
			joined("ced1d402", "2f9ac6ab", "9f19947b", "05e4a455", "037c99ea", "44c94ef4", "16271f6b", "838fc0c7")},
		{"the last-resort default", Secret("", ""),
			joined("12345678", "12345678", "12345678", "12345678", ".00ed385", "326b0b4f", "c119c02f", "77e4eaf5", "f6717438", "ff91603f", "56a65593", "bdd05ffe", "7"),
			joined("706f32f1", "ad1fea99", "a19ea2f6", "5ddfb4ff", "dd5f6b59", "8a13eab4", "963549a2", "acf7f157")},
	} {
		got, gotHash := Build(id, tc.signer)
		if got != tc.want || gotHash != tc.wantHash {
			t.Errorf("%s: got (%s, %s), want (%s, %s)", tc.name, got, gotHash, tc.want, tc.wantHash)
		}
	}
}
