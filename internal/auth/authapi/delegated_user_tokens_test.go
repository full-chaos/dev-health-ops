package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The delegated session routes take the bearer the session issue route takes.
// Every kind of user token is refused 401 invalid_credentials at the credential
// gate, through the routes CompatRoutes mounts from the embedded document: no
// credential-row lookup, no session store call, no echo of the token.
func TestDelegatedRoutesRefuseUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	pool, connects := usertokens.CountingPool(t)
	f := newCompatFixture(t)
	f.serviceCreds = servicecred.PG{Pool: pool}
	f.sessions.startErr = errors.New("a user token reached the delegation start")
	f.sessions.endErr = errors.New("a user token reached the delegation end")
	f.buildCompat()
	bodies := map[string]string{
		PatternDelegatedSession:    delegatedBody(f, f.record.ID),
		PatternDelegatedSessionEnd: `{"session_id":"` + f.record.ID.String() + `"}`,
	}
	for path, body := range bodies {
		for _, tok := range tokens {
			response := f.do(http.MethodPost, path, body, map[string]string{"Authorization": "Bearer " + tok.Value})
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
				t.Errorf("%s %s: %d %s, want 401 %s", path, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
		}
	}
	if len(f.sessions.started) != 0 {
		t.Errorf("a user token reached StartDelegation: %d calls", len(f.sessions.started))
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the credential table: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}
