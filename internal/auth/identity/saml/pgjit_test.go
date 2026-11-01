package saml

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestJITRefusalMap(t *testing.T) {
	for _, row := range []struct {
		name     string
		in, want error
	}{
		{"elsewhere", identity.ErrMemberElsewhere, ErrNotOrgMember},
		{"suspended org", identity.ErrOrganizationSuspended, ErrNotOrgMember},
		{"status kept", identity.ErrMembershipDiverged, ErrNotOrgMember},
		{"inactive", identity.ErrAccountInactive, ErrAccountDisabled},
		{"identity taken", identity.ErrIdentityTaken, ErrIdentityAmbiguous},
		{"email taken", identity.ErrEmailTaken, ErrEmailTaken},
	} {
		if got := mapJITError(row.in); !errors.Is(got, row.want) {
			t.Errorf("%s: %v, want %v", row.name, got, row.want)
		}
	}
}
