package identity

import "testing"

// The login key a copy is written with (copyproject.LoginKey) is the key the
// standalone projection uses: NormalizeEmail where the address is valid, and
// Python strip(lower()) where pydantic EmailStr refuses it.
func TestStoredLoginKeyMatchesNormalizeEmail(t *testing.T) {
	for _, email := range []string{"A.B@Example.COM", " Spaced@Example.com ", "ünï@Example.org", "not-an-email", "x@localhost", "Name <n@Example.com>"} {
		want, err := NormalizeEmail(email)
		if err != nil {
			continue
		}
		if got := storedLoginKey(email); got != want {
			t.Errorf("storedLoginKey(%q) = %q, want %q", email, got, want)
		}
	}
}
