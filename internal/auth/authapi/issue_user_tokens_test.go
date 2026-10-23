package authapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
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

// The session issue route (POST /internal/v1/sessions) accepts only an auth-session-issuer credential
// (svc_authsess_ + 43 base64url characters). Every kind of user token, as the Authorization bearer, is
// refused 401 invalid_credentials at the credential gate: no credential-row lookup (counted on the real
// servicecred checker's pool), no user or membership read, no principal projection, no session created,
// no session-issued audit row, no echo of the token in the body or the logs.
func TestIssueSessionRefusesUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	pool, connects := usertokens.CountingPool(t)
	f := newCompatFixture(t)
	f.serviceCreds = servicecred.PG{Pool: pool}
	f.buildCompat()
	for _, tok := range tokens {
		response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), map[string]string{"Authorization": "Bearer " + tok.Value})
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s: %d %s, want 401 %s", tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, tok.Kind+" body", response.Body.String(), tokens)
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the credential table: %d connection attempt(s), want 0", got)
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

// The control: a well-formed issuer credential passes the shape gate and goes to the credential table, so
// the same counting pool shows a non-zero count (and the route answers 503, the store being unreachable).
func TestIssueSessionControlAnIssuerShapedCredentialReachesTheLookup(t *testing.T) {
	pool, connects := usertokens.CountingPool(t)
	f := newCompatFixture(t)
	f.serviceCreds = servicecred.PG{Pool: pool}
	f.buildCompat()
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"),
		map[string]string{"Authorization": "Bearer svc_authsess_" + strings.Repeat("A", 43)})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("%d %s, want 503 (the lookup was attempted and the store refused)", response.Code, response.Body)
	}
	if connects.Count() == 0 {
		t.Fatal("an issuer-shaped credential never reached the lookup: the counting pool detects nothing")
	}
}
