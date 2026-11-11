package authapi

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
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

// A revoke that would leave no active platform administrator is refused 409 last_platform_admin. go-api's own guard
// (guards.go refuseSuperuserWrite) has no such refusal: this answer is stricter than go-api's, a known divergence
// (ci/auth_exception_ledger.tsv known-stricter-last-platform-admin) pm keeps or drops.
func TestUsersUpdateRevokeOfTheLastPlatformAdminIsRefused(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.roles.revokeErr = platformrole.ErrLastPlatformAdmin
	body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.super.String(), "is_superuser": false}
	expectRefusal(t, f.post(usersUpdatePath, encode(body), nil), http.StatusConflict, reasonLastPlatformAdmin)
}

// The profile fields (username, full_name, avatar_url) are forwarded to the store as sent.
func TestUsersUpdateForwardsTheProfileFields(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "username": "Neo",
		"full_name": "", "avatar_url": "https://example.test/a.png"}
	if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
		t.Fatalf("profile update: %d %s", response.Code, response.Body)
	}
	got := f.usersAdmin.update
	if got.Username == nil || *got.Username != "Neo" || got.FullName == nil || *got.FullName != "" || got.AvatarURL == nil ||
		*got.AvatarURL != "https://example.test/a.png" {
		t.Fatalf("store got username %v full_name %v avatar_url %v", got.Username, got.FullName, got.AvatarURL)
	}
}

// An erase (the admin soft delete) of the last active platform_admin is refused 409 last_platform_admin, as the demote
// is (pm D6878): the delete takes the platform role write lock exclusively and runs the guard on its transaction.
func TestUsersDeleteOfTheLastPlatformAdminIsRefused(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.roles.lastAdminErr = platformrole.ErrLastPlatformAdmin
	body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.super.String(), "mode": "soft"}
	expectRefusal(t, f.post(usersDeletePath, encode(body), nil), http.StatusConflict, reasonLastPlatformAdmin)
	if len(f.roles.lastAdminChecks) != 1 || f.roles.lastAdminChecks[0] != f.super || f.roles.locks != 1 || f.roles.sharedLocks != 0 {
		t.Fatalf("guard checks %v, exclusive locks %d, shared %d; want one check of the target under the exclusive lock",
			f.roles.lastAdminChecks, f.roles.locks, f.roles.sharedLocks)
	}
}

// An org admin edits a member of its organization with the edit form's unchanged is_superuser:false: that is no
// platform role write, so it is not refused (go-api guards.go:106-109 refuses a non-superuser only for a grant or a
// superuser target; round r1 P1-2).
func TestUsersUpdateUnchangedSuperuserFalseByOrgAdminIsServed(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.users.roles[[2]uuid.UUID{f.target, f.org}] = "member"
	body := map[string]any{"actor_principal_id": f.admin.String(), "principal_id": f.target.String(), "organization_id": f.org.String(),
		"email": "changed@example.test", "is_superuser": false}
	if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
		t.Fatalf("org admin edit with an unchanged is_superuser:false: %d %s, want 200", response.Code, response.Body)
	}
	if len(f.roles.grants)+len(f.roles.revokes) != 0 {
		t.Fatalf("grants %v revokes %v, want none", f.roles.grants, f.roles.revokes)
	}
}
