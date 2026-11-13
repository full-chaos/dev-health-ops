package admin

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Roll window (CHAOS-13684): a new go-api against an auth-service that does not take is_active yet. The old route
// refuses the member as malformed (400); go-api answers its 503 and writes nothing, and the user row is unchanged.
func TestUsersOwnedPatchIsActiveAgainstAnOldAuthServiceIs503(t *testing.T) {
	db, users := ownedDB(activeStored()), &fakeUsersAdmin{err: refusedWith(http.StatusBadRequest, "malformed")}
	code, body := ownedCall{route: "update", body: `{"is_active":false}`}.run(ownedHandlers(db, users, true))
	mustOwnedAnswer(t, code, body, http.StatusServiceUnavailable, unavailableBody())
	mustForwardOnly(t, db, users, "update")
	if in := users.updates[0]; in.IsActive == nil || *in.IsActive {
		t.Fatalf("update request %+v, want is_active false sent", in)
	}
}

// In the same window the web edit forms keep working: they send is_active on every save, and an unchanged one is not
// sent, so the old route takes the rest of the edit.
func TestUsersOwnedPatchWithAnUnchangedIsActiveDoesNotSendIt(t *testing.T) {
	stored := activeStored()
	db := ownedDB(stored)
	name := "New Name"
	after := stored
	after.fullName = &name
	users := &fakeUsersAdmin{onUpdate: func() { db.rows[ownedByIDSQL] = after.row() }}
	code, body := ownedCall{route: "update", body: `{"is_active":true,"full_name":"New Name"}`}.run(ownedHandlers(db, users, true))
	if code != http.StatusOK || !strings.Contains(body, `"full_name":"New Name"`) {
		t.Fatalf("answer %d %s, want 200 with the read-back user", code, body)
	}
	mustForwardOnly(t, db, users, "update")
	if in := users.updates[0]; in.IsActive != nil {
		t.Fatalf("update request sent is_active %v for an unchanged value", *in.IsActive)
	}
}

// A4: PATCH with the stored is_active (no other field) answers 200 and the legacy user body, and sends no is_active
// to auth-service, so auth-service writes no principal audit row and no outbox event for it.
func TestUsersOwnedPatchIsActiveNoOpIsServedWithoutAChange(t *testing.T) {
	for _, active := range []bool{true, false} {
		stored := activeStored()
		stored.active = active
		db, users := ownedDB(stored), &fakeUsersAdmin{}
		body := `{"is_active":false}`
		want := `"is_active":false`
		if active {
			body, want = `{"is_active":true}`, `"is_active":true`
		}
		code, answer := ownedCall{route: "update", body: body}.run(ownedHandlers(db, users, true))
		if code != http.StatusOK || !strings.Contains(answer, want) || !strings.Contains(answer, `"id":"`+provTarget.String()+`"`) {
			t.Fatalf("no-op is_active %v: %d %s, want 200 with the user", active, code, answer)
		}
		mustForwardOnly(t, db, users, "update")
		if in := users.updates[0]; in.IsActive != nil {
			t.Fatalf("no-op is_active %v: request sent is_active", active)
		}
	}
}

// A6: an impersonation session gets the same answer for a PATCH is_active as for a PATCH is_verified on the same
// user: is_active adds no path of its own past the impersonation scope (the 13211 narrowing is the route's guard and
// scope, unchanged here). Checked for a superuser and an org admin caller.
func TestUsersOwnedImpersonatedPatchIsActiveAnswersAsIsVerified(t *testing.T) {
	session := &policy.Impersonation{ID: uuid.New(), AdminUserID: provActor, TargetUserID: provTarget, TargetOrgID: provOrg,
		TargetRole: "owner", ExpiresAt: time.Now().Add(time.Hour)}
	for name, caller := range map[string]*policy.User{"superuser": ownedSuperuser(), "org admin": ownedOrgAdmin()} {
		answers := map[string][2]any{}
		for field, body := range map[string]string{"is_active": `{"is_active":false}`, "is_verified": `{"is_verified":false}`} {
			stored := activeStored()
			stored.verified = true
			db := ownedDB(stored)
			after := stored
			after.active, after.verified = field != "is_active", field != "is_verified"
			users := &fakeUsersAdmin{onUpdate: func() { db.rows[ownedByIDSQL] = after.row() }}
			code, _ := ownedCall{route: "update", body: body, caller: caller, session: session}.run(ownedHandlers(db, users, true))
			answers[field] = [2]any{code, strings.Join(users.verbs, ",")}
		}
		if answers["is_active"] != answers["is_verified"] {
			t.Errorf("%s in an impersonation session: is_active %v, is_verified %v; want the same answer and forward",
				name, answers["is_active"], answers["is_verified"])
		}
	}
}
