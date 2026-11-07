package authapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// userTokensWithIssuer is usertokens.All plus an EdDSA access token minted by this package's real EdDSA
// issuer with a throwaway key (usertokens/withissuer imports authapi, which this package's tests cannot).
func userTokensWithIssuer(t *testing.T) []usertokens.Token {
	t.Helper()
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	issuer.now = func() time.Time { return time.Now().UTC().Truncate(time.Second) }
	org := uuid.New()
	compact, err := issuer.AccessContext(context.Background(), Grant{PrincipalID: uuid.New(), OrganizationID: &org, SessionID: uuid.New(), Role: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	return append(usertokens.All(t), usertokens.Token{Kind: "eddsa-issuer", Value: compact})
}

// The session issue route (POST /internal/v1/sessions) accepts only the go-api workload token. Every kind
// of user token, as the Authorization bearer, is refused 401 invalid_credentials at the credential gate:
// no user or membership read, no principal projection, no session created, no session-issued audit row,
// no echo of the token in the body or the logs.
func TestIssueSessionRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	f := newCompatFixture(t)
	for _, tok := range tokens {
		response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), map[string]string{"Authorization": "Bearer " + tok.Value})
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
	if n := len(f.sessions.created); n != 0 {
		t.Errorf("%d session(s) minted for a user token, want 0", n)
	}
	if len(f.projector.userCalls) != 0 || len(f.projector.orgCalls) != 0 {
		t.Errorf("a user token projected principals: users %v orgs %v", f.projector.userCalls, f.projector.orgCalls)
	}
	for _, event := range f.appended {
		if event.EventType == EventSessionIssued {
			t.Errorf("a session-issued audit row exists for a user token")
		}
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}

// The control: the go-api workload token passes the same gate, so the refusals above are the gate's.
func TestIssueSessionControlTheWorkloadTokenPassesTheGate(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer())
	if response.Code != http.StatusOK || len(f.sessions.created) != 1 {
		t.Fatalf("%d %s, %d session(s): want 200 and one session", response.Code, response.Body, len(f.sessions.created))
	}
}
