package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The session state route (POST /internal/v1/sessions/state) takes the bearer
// the session issue route takes: the go-api workload token. Every kind of user
// token, as the Authorization bearer, is refused 401 invalid_credentials at the
// credential gate, through the routes CompatRoutes mounts from the embedded
// document: no session read (a session read here fails, so reaching it would
// answer 503), no echo of the token in the body or the logs.
func TestSessionStateRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	f := newCompatFixture(t)
	f.sessions.currentErr = errors.New("a user token reached the session read")
	f.buildCompat()
	for _, tok := range tokens {
		response := f.do(http.MethodPost, PatternSessionState, stateBody(f.record.ID.String()), map[string]string{"Authorization": "Bearer " + tok.Value})
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}

// The control: the route is mounted from the embedded document, and the go-api
// workload token passes the same gate and reaches the session read (which fails
// here, so the route answers 503).
func TestSessionStateControlTheWorkloadTokenReachesTheSessionRead(t *testing.T) {
	f := newCompatFixture(t)
	f.sessions.currentErr = errors.New("the session store is down")
	response := f.do(http.MethodPost, PatternSessionState, stateBody(f.record.ID.String()), issuerBearer())
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("%d %s, want 503 (the gate passed and the session read failed)", response.Code, response.Body)
	}
}
