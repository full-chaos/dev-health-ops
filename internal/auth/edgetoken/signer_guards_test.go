package edgetoken

// CHAOS-7907: the key-material guards of the Signer. NewSigner builds under the same rules as New (a short secret, an empty issuer or an empty
// audience is refused), and sign refuses to mint with an empty key. The keys are made in the test.

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func syntheticKey() string { return uuid.NewString() + uuid.NewString() } // 72 characters

// Every case New refuses, NewSigner refuses too: it returns no Signer and an error that does not carry the secret; the valid case builds one.
func TestNewSignerRefusesEveryKeyMaterialCaseNewRefuses(t *testing.T) {
	key := syntheticKey()[:MinSecretLength]
	cases := map[string]struct {
		secret, issuer, audience string
		ok                       bool
	}{
		"valid":              {key, "i", "a", true},
		"31 characters":      {key[1:], "i", "a", false},
		"empty secret":       {"", "i", "a", false},
		"32 multibyte runes": {strings.Repeat("é", MinSecretLength), "i", "a", true},
		"31 multibyte runes": {strings.Repeat("é", MinSecretLength-1), "i", "a", false},
		"empty issuer":       {key, "", "a", false},
		"empty audience":     {key, "i", "", false},
	}
	for name, test := range cases {
		_, newErr := New(test.secret, test.issuer, test.audience)
		if (newErr == nil) != test.ok {
			t.Fatalf("%s: New() = %v, the table is wrong about what New refuses", name, newErr)
		}
		signer, err := NewSigner(test.secret, test.issuer, test.audience)
		if (err == nil) != test.ok || (err == nil) != (signer != nil) {
			t.Errorf("%s: NewSigner() = (%v, %v), want ok=%t as New", name, signer, err, test.ok)
		}
		if err != nil && test.secret != "" && strings.Contains(err.Error(), test.secret) {
			t.Errorf("%s: the error carries the secret", name)
		}
	}
}

// A Signer with no key mints nothing: Access, Refresh and RefreshUntil all return an error and an empty token, never a token signed with an empty
// HMAC key. A Signer with a key mints a token the matching Verifier accepts (the control: the refusal is about the key, not about minting).
func TestSignerRefusesToMintWithAnEmptyKey(t *testing.T) {
	now := time.Now()
	access := AccessClaims{UserID: "u", Email: "e@example.test", OrgID: "o", Role: "member"}
	refresh := RefreshClaims{UserID: "u", OrgID: "o", FamilyID: "f"}
	for _, secret := range [][]byte{nil, {}} {
		empty := &Signer{secret: secret, issuer: "i", audience: "a"}
		if token, err := empty.Access(access, now, "jti"); err == nil || token != "" {
			t.Errorf("Access with an empty key = (%q, %v), want an error and no token", token, err)
		}
		if token, err := empty.Refresh(refresh, now, "jti"); err == nil || token != "" {
			t.Errorf("Refresh with an empty key = (%q, %v), want an error and no token", token, err)
		}
		if token, err := empty.RefreshUntil(refresh, now, now.Add(time.Hour), "jti"); err == nil || token != "" {
			t.Errorf("RefreshUntil with an empty key = (%q, %v), want an error and no token", token, err)
		}
	}

	key := syntheticKey()
	signer, err := NewSigner(key, "i", "a")
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Access(access, now, "jti")
	if err != nil || token == "" {
		t.Fatalf("Access with a key = (%q, %v), want a token", token, err)
	}
	verifier, err := New(key, "i", "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(token); err != nil {
		t.Fatalf("the token of a keyed signer is refused by the matching verifier: %v", err)
	}
}
