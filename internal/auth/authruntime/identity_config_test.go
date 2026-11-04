package authruntime

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestIdentityConfigCarriesTheUsersOwnedSwitch(t *testing.T) {
	for _, users := range []bool{true, false} {
		got := identityConfig(authconfig.Config{UsersOwned: users}, nil, nil, authschema.ValidatedIdentifier{})
		if got.UsersOwnedByAuth != users {
			t.Fatalf("users=%v: got users=%v", users, got.UsersOwnedByAuth)
		}
	}
}
