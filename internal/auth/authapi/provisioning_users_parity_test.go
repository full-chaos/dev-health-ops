package authapi

import (
	"net/http"
	"testing"
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

