package admin

import (
	"net/http"
	"strings"
	"testing"
)

// CHAOS-13684 (pm D7015, product rule 1): while users are owned, PATCH /admin/users/{user_id} with a real is_active
// change is served as legacy served it (users.py update_user: 200 and the user body), both ways: go-api forwards the
// change to auth-service and answers its read-back. A1 is the deactivate, A3 the reactivate.
func TestUsersOwnedPatchIsActiveIsServed(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, rolesOwned := range []bool{true, false} {
			stored := activeStored()
			stored.active = !active
			db := ownedDB(stored)
			after := stored
			after.active = active
			users := &fakeUsersAdmin{onUpdate: func() { db.rows[ownedByIDSQL] = after.row() }}
			body := `{"is_active":false}`
			if active {
				body = `{"is_active":true}`
			}
			code, answer := ownedCall{route: "update", body: body}.run(ownedHandlers(db, users, rolesOwned))
			want := `"is_active":false`
			if active {
				want = `"is_active":true`
			}
			if code != http.StatusOK || !strings.Contains(answer, want) || !strings.Contains(answer, `"id":"`+provTarget.String()+`"`) {
				t.Fatalf("is_active %v, roles owned %v: %d %s, want 200 with the read-back user and %s", active, rolesOwned, code, answer, want)
			}
			mustForwardOnly(t, db, users, "update")
			if in := users.updates[0]; in.IsActive == nil || *in.IsActive != active || in.Principal != provTarget || in.Actor != provActor {
				t.Fatalf("is_active %v, roles owned %v: update request %+v", active, rolesOwned, in)
			}
		}
	}
}
