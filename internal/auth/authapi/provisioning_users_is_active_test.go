package authapi

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// CHAOS-13684 (pm D7015): the users update route takes is_active, so go-api can serve PATCH is_active while users
// are owned. Both directions take the shared platform role lock and run no last platform_admin guard: legacy's PATCH
// set is_active with none (users.py UserService.update at 64efc37bb^, product rule 1). The soft delete keeps its guard.
func TestUsersUpdateTakesIsActive(t *testing.T) {
	t.Parallel()
	for _, active := range []bool{false, true} {
		f := newUsersAdminFixture(t)
		body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_active": active}
		if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
			t.Fatalf("update is_active %v: %d %s, want 200", active, response.Code, response.Body)
		}
		if got := f.usersAdmin.update.Active; got == nil || *got != active {
			t.Fatalf("update is_active %v: request Active = %v", active, got)
		}
		if len(f.roles.lastAdminChecks) != 0 || f.roles.locks != 0 || f.roles.sharedLocks != 1 {
			t.Fatalf("is_active %v: guard checks %v, exclusive %d, shared %d; want no guard and one shared lock",
				active, f.roles.lastAdminChecks, f.roles.locks, f.roles.sharedLocks)
		}
	}
}

// PATCH is_active false on the last platform admin is served as legacy served it (200): the update path runs no last
// platform_admin guard, so a guard that would refuse is never asked.
func TestUsersUpdateDeactivatesTheLastPlatformAdminAsLegacy(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.roles.held[f.target] = true // the target holds platform_admin; the guard would call it the last one
	f.roles.lastAdminErr = platformrole.ErrLastPlatformAdmin
	body := map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "is_active": false}
	if response := f.post(usersUpdatePath, encode(body), nil); response.Code != http.StatusOK {
		t.Fatalf("deactivate the last platform admin: %d %s, want 200 as legacy", response.Code, response.Body)
	}
	if len(f.roles.lastAdminChecks) != 0 {
		t.Fatalf("deactivate ran the last platform_admin guard on %v", f.roles.lastAdminChecks)
	}
}
