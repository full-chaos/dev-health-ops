//go:build integration

package session

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/superuserwriter"
)

// createSocialUser inserts as a role that cannot write is_superuser, with the
// switch on and off, and the row reads false.
func TestCreateSocialUserOmitsIsSuperuser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	writer := superuserwriter.Pool(ctx, t, env.Admin, env.URI)
	for _, owned := range []bool{true, false} {
		h := handlers{Deps: Deps{PlatformRoles: policy.PlatformRoleRead{Owned: owned, AuthSchema: "auth"}}.withDefaults()}
		tx, err := writer.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		user, err := h.createSocialUser(ctx, tx, &oauthprovider.UserInfo{}, "social-"+time.Now().Format("150405.000000")+"@example.test", "github", "12345")
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("owned %v: %v", owned, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var superuser bool
		if err := env.Admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, user.ID).Scan(&superuser); err != nil || superuser {
			t.Errorf("owned %v: is_superuser %v err %v, want false", owned, superuser, err)
		}
	}
}
