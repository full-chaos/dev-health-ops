package saml

import (
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestSAMLLookupErrorNamesNonMemberAndSharedNameID(t *testing.T) {
	other := errors.New("database down")
	for in, want := range map[error]error{
		identity.ErrLegacyNotMember:                      ErrNotOrgMember,
		fmt.Errorf("w: %w", identity.ErrLegacyNotMember): ErrNotOrgMember,
		identity.ErrMultipleUsers:                        ErrIdentityAmbiguous,
		fmt.Errorf("w: %w", identity.ErrMultipleUsers):   ErrIdentityAmbiguous,
		other: other,
	} {
		if got := LookupError(in); !errors.Is(got, want) {
			t.Errorf("LookupError(%v) = %v, want %v", in, got, want)
		}
	}
	if got := LookupError(nil); got != nil {
		t.Errorf("LookupError(nil) = %v, want nil", got)
	}
}
