package authruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestIdentityConfigCarriesBothOwnershipSwitches(t *testing.T) {
	for _, tc := range []struct{ users, orgs bool }{{true, true}, {true, false}, {false, true}, {false, false}} {
		got := identityConfig(authconfig.Config{UsersOwned: tc.users, OrgsOwned: tc.orgs}, nil, nil, authschema.ValidatedIdentifier{})
		if got.UsersOwnedByAuth != tc.users || got.OrgsOwnedByAuth != tc.orgs {
			t.Fatalf("users=%v orgs=%v: got users=%v orgs=%v", tc.users, tc.orgs, got.UsersOwnedByAuth, got.OrgsOwnedByAuth)
		}
	}
}
