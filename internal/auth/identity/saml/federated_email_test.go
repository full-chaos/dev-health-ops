package saml

import (
	"errors"
	"testing"
)

func TestIdentityStoreNormalizeEmailRefusesTheReplacementCharacter(t *testing.T) {
	for _, raw := range []string{"e�@example.com", "�@example.com"} {
		if got, err := (IdentityStore{}).NormalizeEmail(raw); !errors.Is(err, ErrInvalidEmailAddress) {
			t.Errorf("NormalizeEmail(%q) = %q, %v; want ErrInvalidEmailAddress", raw, got, err)
		}
	}
}
