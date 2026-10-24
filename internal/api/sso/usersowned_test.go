package sso

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// The account create of an SSO or OAuth sign-in refuses while auth-service
// owns users, before it opens a transaction (no Pool here).
func TestUsersOwnedRefusesTheSSOAccountCreate(t *testing.T) {
	h := handlers{Deps: Deps{UsersOwned: true}}
	_, _, err := h.createAccountAndMembership(context.Background(), &providerRow{}, ssoNewAccount{email: "a@example.test", authProvider: "oidc"})
	if !errors.Is(err, usersowned.ErrRefused) {
		t.Fatalf("createAccountAndMembership = %v, want usersowned.ErrRefused", err)
	}
}
