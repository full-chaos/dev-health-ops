package authapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// With a 10-minute lifetime both tokens of a grant live 600 s, expires_in says 600, and the refresh answer is a
// refresh-response.v2 the contract reader accepts. The default issuer keeps the 60-minute ceiling.
func TestEdDSAAccessLifetimeSetsBothTokensAndExpiresIn(t *testing.T) {
	signer, _, _ := fileSigner(t)
	def := newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
	if def.ExpiresIn() != 3600 {
		t.Fatalf("default ExpiresIn %d, want 3600", def.ExpiresIn())
	}
	issuer, err := NewEdDSAIssuer(testSecret, testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer, WithAccessLifetime(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := issuer.Tokens(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"})
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"access": tokens.Access, "session": tokens.Session} {
		if c := claimsOf(t, token); c.Exp-c.Iat != 600 {
			t.Errorf("%s token lifetime %d, want 600", name, c.Exp-c.Iat)
		}
	}
	if issuer.ExpiresIn() != 600 {
		t.Fatalf("ExpiresIn %d, want 600", issuer.ExpiresIn())
	}

	f := newFixtureWith(t, issuer)
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(7*24*time.Hour - time.Second),
	}}
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code != http.StatusOK {
		t.Fatalf("refresh %d: %s", response.Code, response.Body)
	}
	v2, err := contracts.ParseRefreshResponse(repoRoot(t), response.Body.Bytes())
	if err != nil || v2.ExpiresIn != 600 {
		t.Fatalf("refresh answer: %v, expires_in %d", err, v2.ExpiresIn)
	}
}

// A lifetime outside 1 minute .. the access-token.v1 ceiling cannot build an issuer.
func TestEdDSAAccessLifetimeOutsideTheCeilingIsRefused(t *testing.T) {
	signer, _, _ := fileSigner(t)
	for _, lifetime := range []time.Duration{0, -time.Minute, 30 * time.Second, 61 * time.Minute} {
		if _, err := NewEdDSAIssuer(testSecret, testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer, WithAccessLifetime(lifetime)); err == nil {
			t.Errorf("lifetime %s built an issuer", lifetime)
		}
	}
}

// The go-api compatible refresh route answers the issuer's lifetime: under eddsa with 10 minutes its expires_in
// and its access token's exp - iat are both 600.
func TestCompatRefreshUnderEdDSAStatesTheConfiguredLifetime(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer, err := NewEdDSAIssuer(testSecret, testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer, WithAccessLifetime(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	f := compatFixtureWith(t, issuer)
	f.sessions.result = accepted(f.record)
	f.buildCompat()
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("compat refresh %d: %s", response.Code, response.Body)
	}
	answer := decodeObject(t, response.Body.Bytes())
	token, _ := answer["access_token"].(string)
	if answer["expires_in"] != float64(600) || token == "" {
		t.Fatalf("compat refresh answer expires_in %v, want 600", answer["expires_in"])
	}
	if c := claimsOf(t, token); c.Exp-c.Iat != 600 {
		t.Fatalf("compat refresh access token lifetime %d, want 600", c.Exp-c.Iat)
	}
}
