package authapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// acrRefusal is the refusal of an ACR-audience token at the session routes,
// as the error.v1 reason_code and the refused-counter reason, for the
// authverify this module pins. While authverify knows no ACR audience
// (v0.33.0) the token fails its claims contract (malformed). Once it knows them
// (the release of CHAOS-12249) the token fails the verifier's audience pin
// (audience, counted claims). The reason tells the two stages apart, so a pin
// bump runs the cell against the stage it ships.
func acrRefusal(t *testing.T) (code, reason string) {
	t.Helper()
	config := authverify.VerifierConfig{Issuer: "https://auth.example.test", Audience: AccessTokenAudience, Keys: authverify.StaticKeys{}}
	if _, err := authverify.NewVerifier(config); err != nil {
		t.Fatalf("an auth-service verifier did not build: %v", err)
	}
	config.Audience = contracts.ACRAPIAudience
	if _, err := authverify.NewVerifier(config); err == nil {
		return "audience", accessverify.ReasonClaims
	}
	return reasonMalformed, accessverify.ReasonMalformed
}

// TestAnACRAudienceTokenIsRefusedAtTheSessionRoutesBeforeAnyRead is G-43
// condition 1 at auth-service (ACP-ADR-08 amendment): an access token with aud
// acr-api or acr-mcp is a 401 at every session route, before the session is
// read or revoked. The positive control is the same claims with auth-service's
// audience and scope, served, and the ACR claims are a valid access-token.v1
// document, so the refusal comes from the audience.
func TestAnACRAudienceTokenIsRefusedAtTheSessionRoutesBeforeAnyRead(t *testing.T) {
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/sessions/current"},
		{http.MethodPost, "/v1/sessions/revoke"},
		{http.MethodPost, "/v1/sessions/revoke-all"},
	}
	for _, acr := range []struct{ aud, scope string }{
		{contracts.ACRAPIAudience, "acr:api"},
		{contracts.ACRMCPAudience, "acr:mcp"},
	} {
		t.Run(acr.aud, func(t *testing.T) {
			a := newAccessFixture(t)
			if r := a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(a.claims())); r.Code != http.StatusOK {
				t.Fatalf("positive control: current = %d, body %s", r.Code, r.Body)
			}

			c := a.claims()
			c.Aud, c.Scope = acr.aud, acr.scope
			document, err := json.Marshal(contracts.AccessToken{Header: contracts.AccessTokenHeader{Alg: "EdDSA", Typ: "at+jwt", Kid: "auth-example-2026-10"}, Claims: c})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := contracts.ParseAccessToken(repoRoot(t), document); err != nil {
				t.Fatalf("the %s claims are not a valid access-token.v1 document: %v", acr.aud, err)
			}
			// Any session read now fails loudly, so a 401 proves none happened.
			a.sessions.currentErr = errors.New("a session read")
			token := a.signClaims(c)
			code, reason := acrRefusal(t)
			for _, route := range routes {
				expectError(t, a.do(route.method, route.path, "", token), http.StatusUnauthorized, code)
				if got := a.counter(accessverify.RefusedMetric, map[string]string{"service": AccessService, "profile": route.method + " " + route.path, "reason": reason}); got != 1 {
					t.Errorf("refused %s %s = %d, want 1", route.method, route.path, got)
				}
			}
			if len(a.sessions.revoked) != 0 || len(a.sessions.revokedAll) != 0 {
				t.Fatalf("a session was revoked: %v %v", a.sessions.revoked, a.sessions.revokedAll)
			}
		})
	}
}
