package authapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The authorize routes take the workload token of a policy-enforcement point.
// Every kind of user token, as the Authorization bearer, is refused 401
// invalid_credentials at the caller gate, before any body byte is read: the
// workload verifier is never called (counted on the verifier), no state is
// read, and no token is echoed in the body.
func TestAuthorizeRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	for _, pattern := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		a := newAdmitFixture(t)
		for _, tok := range tokens {
			rec, body := a.do(a.server, pattern, tok.Value, a.payload(pattern), true)
			if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), reasonInvalidCredentials) || body.n != 0 {
				t.Errorf("%s %s: %d %s, body bytes read %d; want 401 %s and 0 bytes read", pattern, tok.Kind, rec.Code, rec.Body, body.n, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, pattern+" "+tok.Kind+" body", rec.Body.String(), tokens)
		}
		if a.verifier.calls != 0 || a.state.reads != 0 {
			t.Errorf("%s: a user token reached the workload verifier (%d) or the state (%d); want 0 and 0", pattern, a.verifier.calls, a.state.reads)
		}
	}
}

// The control: the same route, with a workload token of a listed caller, is decided.
func TestAuthorizeControlAWorkloadTokenIsDecided(t *testing.T) {
	a := newAdmitFixture(t)
	rec, _ := a.do(a.server, PatternAuthorize, a.workloadToken(nil), a.payload(PatternAuthorize), false)
	if rec.Code != http.StatusOK || a.state.reads != 1 {
		t.Fatalf("%d %s, %d state transactions; want 200 and 1", rec.Code, rec.Body, a.state.reads)
	}
}
