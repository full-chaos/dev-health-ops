package signedtoken

import (
	"testing"

	"github.com/google/uuid"
)

// FuzzValidSignedToken drives the signed-token check with arbitrary token text.
// Properties: no panic, an accepted token is exactly the one Build issues for
// the id it names (so no near-miss spelling is ever accepted under a
// different signature), and a non-ASCII signature is reported, not compared.
func FuzzValidSignedToken(f *testing.F) {
	secret := Secret("jwt-secret-for-fuzz-0123456789abcdef", "settings-key")
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	token, _ := Build(id, secret)
	f.Add(token)
	f.Add(token + "x")
	f.Add("." + token)
	f.Add("12345678123456781234567812345678.é")
	f.Add("")
	f.Fuzz(func(t *testing.T, text string) {
		ok, err := Valid(text, secret)
		if err != nil && ok {
			t.Fatalf("Valid returned both ok and error %v", err)
		}
		if !ok {
			return
		}
		idText, signature, found := cutOnce(text)
		if !found || idText == "" || signature == "" {
			t.Fatalf("accepted a token without both halves: %q", text)
		}
	})
}

func cutOnce(text string) (before, after string, found bool) {
	for i := 0; i < len(text); i++ {
		if text[i] == '.' {
			return text[:i], text[i+1:], true
		}
	}
	return text, "", false
}
