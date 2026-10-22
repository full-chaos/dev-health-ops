package session

import "testing"

func TestUnstoredCredentialsAreWellFormedAndDistinct(t *testing.T) {
	a, err := NewUnstoredCredential()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewUnstoredCredential()
	if err != nil {
		t.Fatal(err)
	}
	if !wellFormed(a) || !wellFormed(b) || a.Reveal() == b.Reveal() {
		t.Fatalf("credentials %q %q", a.Reveal(), b.Reveal())
	}
}
