//go:build integration

package sso

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While owned, the OAuth and SAML sign-in reads of an existing user
// (oauth.go and provision.go, through signInUser) take is_superuser from the
// auth platform role, so the login token's claim and needs_onboarding follow
// it: a public-only superuser reads false, an assignment holder true. Off, it
// is public is_superuser.
func TestSignInUserReadsThePlatformRoleWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	publicOnly := env.User(ctx, t, true, false)
	assigned := env.User(ctx, t, false, true)
	for _, owned := range []bool{true, false} {
		h := handlers{Deps: Deps{Pool: env.Reader, PlatformRoles: policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth"}}}
		for who, want := range map[uuid.UUID]bool{publicOnly: !owned, assigned: owned} {
			for _, columns := range []string{`id, email, username, full_name, avatar_url, `, `id, email, username, full_name, `} {
				var id uuid.UUID
				var email string
				var username, fullName, avatarURL *string
				var isSuperuser bool
				var tokenVersion int
				dest := []any{&id, &email, &username, &fullName}
				if columns == `id, email, username, full_name, avatar_url, ` {
					dest = append(dest, &avatarURL)
				}
				dest = append(dest, &isSuperuser, &tokenVersion)
				if err := h.signInUser(ctx, columns, who, dest...); err != nil || isSuperuser != want {
					t.Errorf("owned %v, user %s, columns %q: superuser %v err %v, want %v", owned, who, columns, isSuperuser, err, want)
				}
			}
		}
	}
}
