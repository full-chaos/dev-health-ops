package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

type lifetimeIssuer struct{ expiresIn int64 }

func (f lifetimeIssuer) Issue(context.Context, *http.Request, sessionissue.Request) (sessionissue.Pair, error) {
	return sessionissue.Pair{Access: "issued.access.token", Refresh: "dhr1_issued", ExpiresIn: f.expiresIn}, nil
}

// A sign-in answer built from auth-service's issue pair states the pair's expires_in (the access token's real
// lifetime, 600 under a 10-minute eddsa token); go-api's own mint and an issue pair of 3600 keep 3600.
func TestSignInAnswersStateThePairsExpiresIn(t *testing.T) {
	user := &userRow{ID: uuid.New(), Email: "alice@example.com"}
	for _, want := range []int64{600, 3600} {
		h := handlers{Deps: Deps{Issuer: lifetimeIssuer{expiresIn: want}}}
		pair, err := h.issueMembershipTokens(context.Background(), nil, httptest.NewRequest(http.MethodPost, "/", nil), user, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := answerExpiresIn(t, loginResponse(pair, false, userInfo(user, nil), nil)); got != strconv.FormatInt(want, 10) {
			t.Errorf("issue pair of %d: login expires_in %s", want, got)
		}
	}
	local := tokenPair{access: "a", refresh: "r"}
	if got := answerExpiresIn(t, loginResponse(local, false, userInfo(user, nil), nil)); got != strconv.FormatInt(accessExpiresIn, 10) {
		t.Errorf("go-api's own pair: expires_in %s, want %d", got, accessExpiresIn)
	}
}

func answerExpiresIn(t *testing.T, out *pyjson.Object) string {
	t.Helper()
	value, ok := out.Get("expires_in")
	if !ok {
		t.Fatal("no expires_in")
	}
	text, err := pyjson.Dumps(value)
	if err != nil {
		t.Fatal(err)
	}
	return text
}
