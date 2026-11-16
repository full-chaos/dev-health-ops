package admin

import (
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

var ownedForce = ownedCall{route: "delete", query: "?force=true"}

// R12: while users are owned, ?force=true is forwarded to auth-service as mode force after go-api's own checks, and the
// answer is go-api's force delete answer, {"deleted": true}; an inactive user is erased too (no 404 for a force delete).
func TestUsersOwnedForceDeleteForwardsToAuth(t *testing.T) {
	for _, stored := range []ownedStored{activeStored(), {email: "a@example.com"}} {
		db, users := ownedDB(stored), &fakeUsersAdmin{deleted: authprovision.DeletedUser{WasActive: stored.active}}
		code, body := ownedForce.run(ownedHandlers(db, users, true))
		mustOwnedAnswer(t, code, body, http.StatusOK, `{"deleted":true}`)
		mustForwardOnly(t, db, users, "delete")
		if in := users.deletes[0]; in.Mode != "force" || in.Principal != provTarget || in.Actor != provActor {
			t.Fatalf("delete request %+v, want mode force for the target by the actor", in)
		}
	}
}

// R13: each auth refusal of the erase maps to go-api's answer: the not-owned force delete's 404, 403 and 409, the
// reference_blocked conflict in the 409 form of the owned path, and the 503 for anything else.
func TestUsersOwnedForceDeleteReasonsMapToGoAPIAnswers(t *testing.T) {
	cases := map[string]struct {
		err  error
		code int
		body string
	}{
		"principal_not_found": {refusedWith(404, "principal_not_found"), 404, `{"detail":"User not found"}`},
		"superuser_required":  {refusedWith(403, "superuser_required"), 403, `{"detail":"Superuser access required"}`},
		"last_platform_admin": {refusedWith(409, "last_platform_admin"), 409,
			`{"detail":{"message":"the user-admin routes refuse removing the last active platform_admin, on demote and on erase","reason":"last_platform_admin"}}`},
		"reference_blocked": {refusedWith(409, "reference_blocked"), 409,
			`{"detail":{"message":"identity: a row that names the user blocks its erase","reason":"reference_blocked"}}`},
		"store_unavailable": {refusedWith(503, "store_unavailable"), 503, unavailableBody()},
		"transport":         {errors.New("transport"), 503, unavailableBody()},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: c.err}
			code, body := ownedForce.run(ownedHandlers(db, users, true))
			mustOwnedAnswer(t, code, body, c.code, c.body)
			mustForwardOnly(t, db, users, "delete")
		})
	}
}

// R14: the force delete runs go-api's checks first, as the not-owned force delete does: a malformed force value is
// 422, an unknown user 404 and a user outside the org admin's organization 404, and auth-service is not called.
func TestUsersOwnedForceDeleteChecksExistenceAndScopeFirst(t *testing.T) {
	missing := &provisionDB{rows: provisionRowsFor()}
	users := &fakeUsersAdmin{}
	code, body := ownedForce.run(ownedHandlers(missing, users, true))
	mustOwnedAnswer(t, code, body, http.StatusNotFound, `{"detail":"User not found"}`)
	mustNoForward(t, missing, users)

	outside := ownedDB(activeStored())
	outside.rows["SELECT EXISTS(SELECT 1 FROM memberships WHERE org_id = $1 AND user_id = $2)"] = []any{false}
	users = &fakeUsersAdmin{}
	code, body = ownedCall{route: "delete", query: "?force=true", caller: ownedOrgAdmin()}.run(ownedHandlers(outside, users, true))
	mustOwnedAnswer(t, code, body, http.StatusNotFound, `{"detail":"User not found"}`)
	mustNoForward(t, outside, users)

	db := ownedDB(activeStored())
	users = &fakeUsersAdmin{}
	code, _ = ownedCall{route: "delete", query: "?force=maybe"}.run(ownedHandlers(db, users, true))
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("force=maybe: %d, want 422", code)
	}
	mustNoForward(t, db, users)
}
