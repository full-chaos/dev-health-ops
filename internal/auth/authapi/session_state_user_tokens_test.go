package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The session state route (POST /internal/v1/sessions/state) takes the bearer
// the session issue route takes: the session issuer credential or the go-api
// workload token. Every kind of user token, as the Authorization bearer, is
// refused 401 invalid_credentials at the credential gate, through the routes
// CompatRoutes mounts from the embedded document: no credential-row lookup
// (counted on the real servicecred checker's pool), no session read (a session
// read here fails, so reaching it would answer 503), no echo of the token in
// the body or the logs.
func TestSessionStateRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	pool, connects := usertokens.CountingPool(t)
	f := newCompatFixture(t)
	f.serviceCreds = servicecred.PG{Pool: pool}
	f.sessions.currentErr = errors.New("a user token reached the session read")
	f.buildCompat()
	for _, tok := range tokens {
		response := f.do(http.MethodPost, PatternSessionState, stateBody(f.record.ID.String()), map[string]string{"Authorization": "Bearer " + tok.Value})
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the credential table: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}

// The control: the route is mounted from the embedded document, and a
// well-formed issuer credential passes the shape gate and reaches the
// credential table (the store being unreachable, the route answers 503).
func TestSessionStateControlAnIssuerShapedCredentialReachesTheLookup(t *testing.T) {
	pool, connects := usertokens.CountingPool(t)
	f := newCompatFixture(t)
	f.serviceCreds = servicecred.PG{Pool: pool}
	f.buildCompat()
	response := f.do(http.MethodPost, PatternSessionState, stateBody(f.record.ID.String()),
		map[string]string{"Authorization": "Bearer svc_authsess_" + strings.Repeat("A", 43)})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("%d %s, want 503 (the lookup was attempted and the store refused)", response.Code, response.Body)
	}
	if connects.Count() == 0 {
		t.Fatal("an issuer-shaped credential never reached the lookup: the counting pool detects nothing")
	}
}
