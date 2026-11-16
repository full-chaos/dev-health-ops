package authapi

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

func forceBody(f *provFixture) string {
	return encode(map[string]any{"actor_principal_id": f.super.String(), "principal_id": f.target.String(), "mode": "force"})
}

// A row that names the user blocks the erase: 409 reference_blocked, a named conflict go-api answers in its 409 form.
func TestUsersForceDeleteAnswersReferenceBlocked(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	f.usersAdmin.err = identity.ErrReferenceBlocked
	expectRefusal(t, f.post(usersDeletePath, forceBody(f), nil), http.StatusConflict, reasonReferenceBlocked)
}

// The erase runs the soft delete's actor rule and last platform_admin guard, and revokes the target's own
// platform_admin assignment on the same transaction (RevokeOn, never the store's own Revoke); any other revoke refusal
// refuses the erase.
func TestUsersForceDeleteRevokesTheTargetsPlatformAdmin(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	if response := f.post(usersDeletePath, forceBody(f), nil); response.Code != http.StatusOK {
		t.Fatalf("force delete of a platform admin: %d %s, want 200", response.Code, response.Body)
	}
	if len(f.roles.revokes) != 1 || f.roles.revokes[0] != f.target || f.roles.direct != 0 {
		t.Errorf("revokes %v (direct %d), want one RevokeOn of the target", f.roles.revokes, f.roles.direct)
	}
	if len(f.roles.lastAdminChecks) != 1 || f.roles.lastAdminChecks[0] != f.target {
		t.Errorf("last platform_admin checks %v, want the target once", f.roles.lastAdminChecks)
	}

	// A user with no platform_admin: the revoke answers not held, which is no refusal of the erase.
	plain := newUsersAdminFixture(t)
	plain.roles.revokeErr = platformrole.ErrNotHeld
	if response := plain.post(usersDeletePath, forceBody(plain), nil); response.Code != http.StatusOK {
		t.Fatalf("force delete of a plain user: %d %s, want 200", response.Code, response.Body)
	}
	failing := newUsersAdminFixture(t)
	failing.roles.revokeErr = platformrole.ErrGranterNotHeld
	expectRefusal(t, failing.post(usersDeletePath, forceBody(failing), nil), http.StatusForbidden, reasonSuperuserRequired)

	last := newUsersAdminFixture(t)
	last.roles.lastAdminErr = platformrole.ErrLastPlatformAdmin
	expectRefusal(t, last.post(usersDeletePath, forceBody(last), nil), http.StatusConflict, reasonLastPlatformAdmin)
}
