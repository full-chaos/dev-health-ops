package authapi

import (
	"net/http"
	"testing"
)

// The delete route takes mode "force" (go-api's ?force=true while auth-service owns users): the same actor rule as the
// soft delete, then the erase, never the soft delete. The answer names the principal; it was active and is not a
// repeat.
func TestUsersDeleteRouteTakesForceMode(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	response := f.post(usersDeletePath, encode(map[string]any{"actor_principal_id": f.super.String(),
		"principal_id": f.target.String(), "mode": "force"}), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("force delete: %d %s, want 200", response.Code, response.Body)
	}
	answer := usersAnswerOf(t, response.Body.Bytes())
	if answer["principal_id"] != f.target.String() || answer["was_active"] != true || answer["already_done"] != false {
		t.Errorf("force delete answer = %v, want principal_id %s, was_active true, already_done false", answer, f.target)
	}
	if f.usersAdmin.soft.Principal == f.target {
		t.Error("force delete ran the soft delete")
	}
	if f.usersAdmin.calls != 1 {
		t.Errorf("user-admin store calls = %d, want 1 (the erase)", f.usersAdmin.calls)
	}
}

// A mode the route does not know is still 400 malformed, before any row is read.
func TestUsersDeleteRouteRefusesAnUnknownMode(t *testing.T) {
	t.Parallel()
	f := newUsersAdminFixture(t)
	expectRefusal(t, f.post(usersDeletePath, encode(map[string]any{"actor_principal_id": f.super.String(),
		"principal_id": f.target.String(), "mode": "hard"}), nil), http.StatusBadRequest, reasonMalformed)
	if f.usersAdmin.calls != 0 {
		t.Errorf("user-admin store calls = %d, want 0", f.usersAdmin.calls)
	}
}
