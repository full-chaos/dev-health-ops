package oidc

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func TestJITRefusalMap(t *testing.T) {
	for _, row := range []struct {
		name     string
		in, want error
	}{
		{"elsewhere", identity.ErrMemberElsewhere, ErrNotOrgMember},
		{"suspended org", identity.ErrOrganizationSuspended, ErrNotOrgMember},
		{"status kept", identity.ErrMembershipDiverged, ErrNotOrgMember},
		{"inactive", identity.ErrAccountInactive, ErrPrincipalDisabled},
		{"session principal", session.ErrPrincipalUnavailable, ErrPrincipalDisabled},
		{"identity taken", identity.ErrIdentityTaken, ErrIdentityConflict},
		{"email taken", identity.ErrEmailTaken, ErrIdentityConflict},
	} {
		if got := mapJITError(row.in); !errors.Is(got, row.want) {
			t.Errorf("%s: %v, want %v", row.name, got, row.want)
		}
	}
}
