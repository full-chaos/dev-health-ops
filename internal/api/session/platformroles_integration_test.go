//go:build integration

package session

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While owned, the session users reads (login, refresh, switch-org, the
// claims and needs_onboarding they feed) take is_superuser from the auth
// platform role.
func TestSessionUserReadsFollowThePlatformRoleWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	publicOnly := env.User(ctx, t, true, false)
	assigned := env.User(ctx, t, false, true)
	for _, owned := range []bool{true, false} {
		h := handlers{Deps: Deps{PlatformRoles: policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth"}}}
		for who, want := range map[uuid.UUID]bool{publicOnly: !owned, assigned: owned} {
			tx, err := env.Reader.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			user, err := h.userByID(ctx, tx, who)
			_ = tx.Rollback(ctx)
			if err != nil || user == nil || user.IsSuperuser != want {
				t.Errorf("owned %v, user %s: %+v %v, want superuser %v", owned, who, user, err, want)
			}
		}
	}
}
