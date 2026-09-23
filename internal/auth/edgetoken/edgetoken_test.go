package edgetoken

import (
	"strings"
	"testing"
)

func TestNewRefusesWeakOrIncompleteKeyMaterial(t *testing.T) {
	key := strings.Repeat("k", MinSecretLength)
	cases := map[string]struct {
		secret, issuer, audience string
		ok                       bool
	}{
		"valid":              {key, "i", "a", true},
		"31 characters":      {key[1:], "i", "a", false},
		"32 multibyte runes": {strings.Repeat("é", MinSecretLength), "i", "a", true},
		"31 multibyte runes": {strings.Repeat("é", MinSecretLength-1), "i", "a", false},
		"empty issuer":       {key, "", "a", false},
		"empty audience":     {key, "i", "", false},
	}
	for name, test := range cases {
		verifier, err := New(test.secret, test.issuer, test.audience)
		if (err == nil) != test.ok || (err == nil) != (verifier != nil) {
			t.Errorf("%s: %v", name, err)
		}
		if err != nil && strings.Contains(err.Error(), test.secret) && test.secret != "" {
			t.Errorf("%s: error carries the secret", name)
		}
	}
}

func TestReasonOf(t *testing.T) {
	if ReasonOf(&Rejection{Reason: ReasonExpired}) != ReasonExpired || ReasonOf(nil) != ReasonMalformed {
		t.Fatal("ReasonOf")
	}
	verifier, _ := New(strings.Repeat("k", MinSecretLength), "i", "a")
	if _, err := verifier.Verify("x"); ReasonOf(err) != ReasonMalformed {
		t.Fatalf("garbage token reason %q", ReasonOf(err))
	}
}
