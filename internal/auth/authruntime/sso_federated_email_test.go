package authruntime

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestSAMLIdentitiesRefuseTheReplacementCharacterInAnEmail(t *testing.T) {
	for _, raw := range []string{"e�@example.com", "�@example.com"} {
		if got, err := (samlIdentities{}).NormalizeEmail(raw); !errors.Is(err, identity.ErrInvalidEmail) {
			t.Errorf("NormalizeEmail(%q) = %q, %v; want ErrInvalidEmail", raw, got, err)
		}
	}
}
