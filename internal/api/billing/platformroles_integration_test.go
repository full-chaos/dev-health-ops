//go:build integration

package billing

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// The entitlement access check passes a superuser into any organization. While
// owned, superuser means the auth platform role: a public-only superuser is
// refused another organization's entitlements, and an assignment holder passes.
func TestEntitlementAccessReadsThePlatformRoleWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	publicOnly := env.User(ctx, t, true, false)
	assigned := env.User(ctx, t, false, true)
	org := uuid.New()
	pgseed.Org(ctx, t, env.Admin, org.String(), "free")
	for _, owned := range []bool{true, false} {
		h := handlers{platformRoles: policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth"}}
		for who, wantPass := range map[uuid.UUID]bool{publicOnly: !owned, assigned: owned} {
			user := &policy.User{UserID: who.String(), ID: who, OrgID: uuid.NewString()}
			answer, err := h.entitlementAccess(ctx, env.Reader, user, org)
			if err != nil || (answer == nil) != wantPass {
				t.Errorf("owned %v, user %s: answer %v err %v, want pass %v", owned, who, answer, err, wantPass)
			}
		}
	}
}
