//go:build integration

package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// While platform roles are owned, the admin users view shows the STORED platform_admin holding, as legacy showed its
// stored is_superuser column: an inactive user whose principal is disabled but still holds the assignment is
// is_superuser true in the view, and holds NO authority (the decision rule reads the principal's state).
func TestOwnedAdminUsersViewShowsTheStoredPlatformAdmin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	store := pgStore{Pool: env.Admin, PlatformRolesOwned: true}
	authority, err := policy.PlatformRoleRead{Owned: true}.Superuser("is_superuser", "users.id")
	if err != nil {
		t.Fatal(err)
	}

	inactiveHolder := env.User(ctx, t, false, true)
	pgseed.SetUserState(ctx, t, env.Admin, inactiveHolder.String(), false, false, 0)
	env.Disable(ctx, t, inactiveHolder)
	for name, c := range map[string]struct {
		id              uuid.UUID
		view, authority bool
	}{
		"inactive stored holder": {inactiveHolder, true, false},
		"active holder":          {env.User(ctx, t, false, true), true, true},
		"no assignment":          {env.User(ctx, t, false, false), false, false},
		"expired assignment":     {env.ExpiredUser(ctx, t, false), false, false},
	} {
		user, err := store.fullUserByID(ctx, c.id)
		if err != nil || user == nil {
			t.Fatalf("%s: view read %v, %v", name, user, err)
		}
		if user.IsSuperuser != c.view {
			t.Errorf("%s: view is_superuser %v, want %v", name, user.IsSuperuser, c.view)
		}
		var held bool
		if err := env.Admin.QueryRow(ctx, `SELECT `+authority+` FROM users WHERE id = $1`, c.id).Scan(&held); err != nil {
			t.Fatalf("%s: authority read: %v", name, err)
		}
		if held != c.authority {
			t.Errorf("%s: authority %v, want %v", name, held, c.authority)
		}
	}
}
