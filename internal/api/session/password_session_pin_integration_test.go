//go:build integration

package session_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// A session signed in before a password change is refused by go-api's
// session check afterwards; one signed in after the write stays live.
func TestPasswordWritesEndEarlierAuthSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st := startStackWithMail(t, ctx, authmail.Config{})
	rig := pinrig.NewPinRig(t, ctx, st.pool, st.pg)
	orgA := st.specs["alice"]["org_id"].(string)
	alice, bob := uuid.MustParse(changeAlice), uuid.MustParse(changeBob)

	check := func(name string, user, before uuid.UUID, wantBefore bool) uuid.UUID {
		t.Helper()
		err := rig.Check(ctx, user, before)
		if got := pinrig.Refused(err); got != wantBefore || (!got && err != nil) {
			t.Fatalf("%s: err = %v, refused = %v, want refused = %v", name, err, got, wantBefore)
		}
		return before
	}

	oldChange := rig.SignIn(t, ctx, alice)
	check("before the change", alice, oldChange, false)
	token := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	if status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", token,
		changeBody(sessionscenario.Password, newPassword)); status != http.StatusOK {
		t.Fatalf("change-password: %d %s", status, body)
	}
	check("older session after the change", alice, oldChange, true)
	newChange := rig.SignIn(t, ctx, alice)
	check("newer session after the change", alice, newChange, false)

	unpinned := rig.SignIn(t, ctx, bob)
	rig.ClearPin(t, ctx, unpinned)
	check("session without a pin", bob, unpinned, true)
}
