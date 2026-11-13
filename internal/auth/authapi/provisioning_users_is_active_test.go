package authapi

import (
	"net/http"
	"testing"
)

// CHAOS-13684 (pm D7015): the users update route takes is_active, so go-api can serve PATCH is_active while users
// are owned. A deactivate (false) is an erase of the login, as the soft delete is: it takes the platform role write
// lock exclusively and runs the last platform_admin guard on the target (pm D6878). A reactivate (true) takes the
// shared lock and no guard.
func TestUsersUpdateTakesIsActive(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	off := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_active": false}
	if response := f.post(usersUpdatePath, encode(off), nil); response.Code != http.StatusOK {
		t.Fatalf("update is_active false: %d %s, want 200", response.Code, response.Body)
	}
	if len(f.roles.lastAdminChecks) != 1 || f.roles.lastAdminChecks[0] != f.target || f.roles.locks != 1 {
		t.Fatalf("deactivate: guard checks %v, exclusive locks %d; want one check of the target under the exclusive lock",
			f.roles.lastAdminChecks, f.roles.locks)
	}
	on := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_active": true}
	if response := f.post(usersUpdatePath, encode(on), nil); response.Code != http.StatusOK {
		t.Fatalf("update is_active true: %d %s, want 200", response.Code, response.Body)
	}
	if len(f.roles.lastAdminChecks) != 1 || f.roles.locks != 1 {
		t.Fatalf("reactivate: guard checks %v, exclusive locks %d; want no new check and no exclusive lock",
			f.roles.lastAdminChecks, f.roles.locks)
	}
}
