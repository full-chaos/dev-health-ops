package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

type sessionTokenIssuer struct{ session string }

func (f sessionTokenIssuer) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{Access: "issued.access.token", Refresh: "dhr1_issued", SessionAccess: f.session}, nil
}

// A sign-in answer hands on auth-service's session-route token as its last member; without one (hs256_compat, or
// go-api minting itself) the answer keeps go-api's exact member list.
func TestSignInAnswersHandOnTheSessionRouteToken(t *testing.T) {
	user := &userRow{ID: uuid.New(), Email: "alice@example.com"}
	for session, want := range map[string]bool{"s.t.u": true, "": false} {
		h := handlers{Deps: Deps{Issuer: sessionTokenIssuer{session: session}}}
		pair, err := h.issueMembershipTokens(context.Background(), nil, httptest.NewRequest(http.MethodPost, "/", nil), user, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := loginResponse(pair, false, userInfo(user, nil), nil)
		keys := out.Keys()
		value, has := out.Get("session_access_token")
		if has != want || (want && (value != session || keys[len(keys)-1] != "session_access_token")) {
			t.Fatalf("session %q: keys %v", session, keys)
		}
		if !want && slices.Contains(keys, "session_access_token") {
			t.Fatalf("an answer without a session token lists %v", keys)
		}
	}
}
