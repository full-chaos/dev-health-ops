package principal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// fakeSessionUsers is fakeUsers plus the auth-service sessions.
type fakeSessionUsers struct {
	*fakeUsers
	row   policy.SessionRow
	found bool
	err   error
	calls int
}

func (f *fakeSessionUsers) SessionState(context.Context, uuid.UUID) (policy.SessionRow, bool, error) {
	f.calls++
	return f.row, f.found, f.err
}

// query-api decides an auth-service token (one that carries sid) as go-api
// does: refused once its session is revoked, and a session lookup it cannot
// read is its own unavailable refusal, never a pass.
func TestEdgeVerifier_ChecksTheSessionOfASidToken(t *testing.T) {
	subject := uuid.MustParse(edgeTestSubject)
	sid := sessionid.Wire(uuid.MustParse("77777777-7777-4777-8777-777777777777"))
	withSID := func(claims map[string]any) { claims["sid"] = sid }
	for _, test := range []struct {
		name  string
		users *fakeSessionUsers
		want  string
	}{
		{name: "live", users: &fakeSessionUsers{fakeUsers: activeUserStore(), row: policy.SessionRow{PrincipalID: subject}, found: true}, want: "ok"},
		{name: "revoked", users: &fakeSessionUsers{fakeUsers: activeUserStore(), row: policy.SessionRow{PrincipalID: subject, Revoked: true}, found: true}, want: "refused"},
		{name: "missing", users: &fakeSessionUsers{fakeUsers: activeUserStore()}, want: "refused"},
		{name: "lookup unavailable", users: &fakeSessionUsers{fakeUsers: activeUserStore(), err: errors.Join(policy.ErrUnavailable, errors.New("dial"))}, want: "unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			claims, err := verifyWithUsers(t, test.users, withSID)
			got := "ok"
			switch {
			case err == nil:
			case policy.IsRefusal(err):
				got = "refused"
			case errors.Is(err, policy.ErrUnavailable):
				got = "unavailable"
			default:
				got = "other"
			}
			if got != test.want {
				t.Fatalf("outcome %s (%v), want %s", got, err, test.want)
			}
			if got != "ok" && claims != nil {
				t.Fatalf("claims returned with a %s outcome: %+v", got, claims)
			}
			if test.users.calls != 1 {
				t.Fatalf("session reads = %d, want 1", test.users.calls)
			}
		})
	}
}
