package authapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The delegated session routes take the bearer the session issue route takes.
// Every kind of user token is refused 401 invalid_credentials at the credential
// gate, through the routes CompatRoutes mounts from the embedded document: no
// credential-row lookup, no read of the body, no session store call, no echo of
// the token.
func TestDelegatedRoutesRefuseUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	if len(tokens) != 15 {
		t.Fatalf("%d user token vectors, want 15", len(tokens))
	}
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
			reader := &countingBody{Reader: strings.NewReader(body)}
			request := httptest.NewRequest(http.MethodPost, path, reader)
			request.Header.Set("Authorization", "Bearer "+tok.Value)
			response := httptest.NewRecorder()
			f.server.ServeHTTP(response, request)
			if reader.reads != 0 {
				t.Errorf("%s %s: body_reads=%d, want 0: the body was read before the bearer was refused", path, tok.Kind, reader.reads)
			}
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
				t.Errorf("%s %s: %d %s, want 401 %s", path, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
		}
	}
	if len(f.sessions.started) != 0 || f.sessions.endCalls != 0 {
		t.Errorf("a user token reached the session store: %d starts, %d ends", len(f.sessions.started), f.sessions.endCalls)
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the credential table: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}
