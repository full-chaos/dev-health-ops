package authapi

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// Every PATCH is one AdminUpdateUser, a no-op and a platform role change only included: go-api bumps updated_at on
// every PATCH that finds the user (pm D6915), and one apply keeps one lock order. The role change runs as the
// update's Superuser step (GrantOn on the update's transaction), never as the platform role store's own commit.
func TestUsersUpdateNoOpAndRoleOnlyGoThroughAdminUpdate(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	noop := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_superuser": false}
	response := f.post(usersUpdatePath, encode(noop), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("no-op update: %d %s", response.Code, response.Body)
	}
	if f.usersAdmin.calls != 1 || f.usersAdmin.update.Superuser != nil {
		t.Fatalf("no-op update: store calls %d, Superuser step set %v; want one AdminUpdateUser with no role step",
			f.usersAdmin.calls, f.usersAdmin.update.Superuser != nil)
	}
	if changed, _ := usersAnswerOf(t, response.Body.Bytes())["changed"].(bool); changed {
		t.Errorf("no-op update answered changed: %s", response.Body)
	}
	roleOnly := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_superuser": true}
	response = f.post(usersUpdatePath, encode(roleOnly), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("role-only update: %d %s", response.Code, response.Body)
	}
	if f.usersAdmin.calls != 2 || f.usersAdmin.update.Superuser == nil {
		t.Fatalf("role-only update: store calls %d, Superuser step set %v; want a second AdminUpdateUser with the role step",
			f.usersAdmin.calls, f.usersAdmin.update.Superuser != nil)
	}
	if f.roles.direct != 0 || len(f.roles.grants) != 1 || f.roles.grants[0] != f.target {
		t.Fatalf("platform role store: own commits %d, grants %v; want one grant to the target on the update's transaction",
			f.roles.direct, f.roles.grants)
	}
	if changed, _ := usersAnswerOf(t, response.Body.Bytes())["changed"].(bool); !changed {
		t.Errorf("role-only update did not answer changed: %s", response.Body)
	}
}

// The update's platform role grant asks to land on a disabled principal, as go-api's admin update grants is_superuser
// to an inactive user (pm D6914).
func TestUsersUpdateGrantAllowsADisabledTarget(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_superuser": true}
	if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
		t.Fatalf("role-only update: %d %s", response.Code, response.Body)
	}
	if len(f.roles.grantInputs) != 1 || !f.roles.grantInputs[0].AllowDisabledTarget {
		t.Fatalf("grant inputs %+v, want one with AllowDisabledTarget", f.roles.grantInputs)
	}
}

// The update decides a platform role change from the stored assignment, whatever the target principal's state: an
// inactive user who still holds platform_admin and is sent is_superuser true is no change (no grant, 200, the touch
// only), and sent is_superuser false is a revoke on the update's transaction, as go-api writes its column (r1 P1).
func TestUsersUpdateDecidesTheRoleChangeFromTheStoredAssignment(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.roles.held = map[uuid.UUID]bool{f.target: true}
	post := func(superuser bool) {
		t.Helper()
		body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_superuser": superuser}
		if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
			t.Fatalf("is_superuser %v on a stored holder: %d %s, want 200", superuser, response.Code, response.Body)
		}
	}
	post(true)
	if len(f.roles.grants) != 0 || f.usersAdmin.update.Superuser != nil {
		t.Fatalf("is_superuser true on a stored holder: grants %v, role step set %v; want no grant and the touch only",
			f.roles.grants, f.usersAdmin.update.Superuser != nil)
	}
	post(false)
	if len(f.roles.revokes) != 1 || f.roles.revokes[0] != f.target || f.roles.direct != 0 || f.usersAdmin.update.Superuser == nil {
		t.Fatalf("is_superuser false on a stored holder: revokes %v, own commits %d, role step set %v; want one revoke of the target on the update's transaction",
			f.roles.revokes, f.roles.direct, f.usersAdmin.update.Superuser != nil)
	}
}
