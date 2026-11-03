//go:build integration

package sso

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/superuserwriter"
)

// createAccountAndMembership runs on a role that cannot write
// users.is_superuser, with the switch on and off: the account is created and
// its row reads is_superuser = false.
func TestCreateAccountAndMembershipOmitsIsSuperuser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	writer := superuserwriter.Pool(ctx, t, env.Admin, env.URI)
	for _, owned := range []bool{true, false} {
		org := uuid.New()
		pgseed.Org(ctx, t, env.Admin, org.String(), "free")
		h := newHandlers(Deps{Pool: writer, PlatformRoles: policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth"}})
		subject := uuid.NewString()
		user, _, err := h.createAccountAndMembership(ctx, &providerRow{OrgID: org.String()}, ssoNewAccount{
			email: "jit-" + subject[:8] + "@example.test", authProvider: "github", externalID: &subject,
		})
		if err != nil {
			t.Fatalf("owned %v: %v", owned, err)
		}
		var superuser bool
		if err := env.Admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, user.id).Scan(&superuser); err != nil || superuser {
			t.Errorf("owned %v: is_superuser %v err %v, want false", owned, superuser, err)
		}
	}
}
