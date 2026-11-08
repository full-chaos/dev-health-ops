package authapi

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// A create with grant_platform_admin grants platform_admin to the new principal inside the create (the store's
// Grant step, under the exclusive platform role write lock), and only for a superuser actor.
func TestUsersCreateGrantsPlatformAdminInTheCreate(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	body := map[string]any{"actor_principal_id": f.super.String(), "email": "boss@example.com", "auth_provider": "local",
		"is_verified": true, "grant_platform_admin": true}
	response := f.post(usersCreatePath, encode(body), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("create with grant: %d %s", response.Code, response.Body)
	}
	if len(f.roles.grants) != 1 || f.roles.grants[0] != f.usersAdmin.createdID {
		t.Fatalf("grants = %v, want one grant to the new principal %s", f.roles.grants, f.usersAdmin.createdID)
	}
	if f.roles.locks != 1 || f.roles.sharedLocks != 0 {
		t.Fatalf("locks exclusive %d shared %d, want the exclusive write lock once", f.roles.locks, f.roles.sharedLocks)
	}
	plain := map[string]any{"actor_principal_id": f.super.String(), "email": "plain@example.com", "auth_provider": "local"}
	if response := f.post(usersCreatePath, encode(plain), nil); response.Code != http.StatusOK || len(f.roles.grants) != 1 {
		t.Fatalf("create without grant: %d %s, grants %v; want 200 and no new grant", response.Code, response.Body, f.roles.grants)
	}
}

// The actor rule runs again on the store's transaction: an org admin demoted between the route's read and the
// store's locks is refused there.
func TestUsersCreateRerunsTheActorRuleUnderTheLocks(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.users.inTx = &fakeProvUsers{states: f.users.states, roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "viewer"}}
	body := map[string]any{"actor_principal_id": f.admin.String(), "email": "m@example.com", "auth_provider": "local",
		"organization_id": f.org.String(), "role": "member"}
	expectRefusal(t, f.post(usersCreatePath, encode(body), nil), http.StatusForbidden, reasonAdminRequired)
}

// An update changes the platform role only when the requested flag differs from what the target holds; a flag equal
// to it with no other change writes nothing.
func TestUsersUpdateSuperuserGrantsAndRevokesOnlyOnAChange(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	post := func(target uuid.UUID, superuser bool) {
		t.Helper()
		body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": target.String(), "is_superuser": superuser}
		if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
			t.Fatalf("update is_superuser %v: %d %s", superuser, response.Code, response.Body)
		}
	}
	post(f.target, false)
	if len(f.roles.grants)+len(f.roles.revokes) != 0 {
		t.Fatalf("an unchanged flag wrote grants %v revokes %v", f.roles.grants, f.roles.revokes)
	}
	post(f.target, true)
	post(f.super, false)
	if len(f.roles.grants) != 1 || f.roles.grants[0] != f.target || len(f.roles.revokes) != 1 || f.roles.revokes[0] != f.super {
		t.Fatalf("grants %v revokes %v, want a grant to the target and a revoke of the superuser", f.roles.grants, f.roles.revokes)
	}
}
