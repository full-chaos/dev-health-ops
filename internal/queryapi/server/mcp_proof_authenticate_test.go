package server

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The proof variant's own identity step, driven with a REAL signed envelope
// (no database): a valid envelope for an allowlisted org is accepted with the
// org alone (no role, no superuser), and the same envelope beside the internal
// identity headers is refused -- the headers acr-api can send must buy nothing
// on this route, even next to a valid credential.
func TestMCPProofAuthenticateAcceptsAnEnvelopeAndRefusesIdentityHeadersBesideIt(t *testing.T) {
	const kid, issuer, audience, org = "proof-test-kid", "dev-health-ops-edge", "query-api", "org-proof"
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub), "kid": kid, "use": "sig", "alg": "EdDSA",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, jwks, 0o600); err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(path, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	claims := principal.Claims{OrgID: org, Role: "admin", SchemaVersion: principal.SupportedSchemaVersion}
	claims.RegisteredClaims = jwt.RegisteredClaims{Issuer: issuer, Audience: jwt.ClaimStrings{audience}, Subject: "u", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
	unsigned := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	unsigned.Header["kid"] = kid
	token, err := unsigned.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}

	base := newMCPHandler(&countingMCPClient{}, nil, allMCPRootsEnabled(), getenvFunc(func(string) string { return "" }))
	handler := newMCPProofHandler(base, allMCPRootsEnabled(), verifier, func(_ context.Context, o string) bool { return o == org })
	proof, ok := handler.(*mcpHandler)
	if !ok {
		t.Fatalf("proof handler is %T", handler)
	}
	request := func(mutate func(*http.Request)) (int, string, string) {
		r := httptest.NewRequest(http.MethodPost, "/query/proof-mcp", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		mutate(r)
		got, status, reason := proof.authenticate(r)
		return status, reason, got.OrgID
	}

	if status, reason, got := request(func(*http.Request) {}); status != 0 || reason != "" || got != org {
		t.Fatalf("a valid envelope for an allowlisted org: status %d reason %q org %q, want it accepted", status, reason, got)
	}
	if got, _, _ := proof.authenticate(func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/query/proof-mcp", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}()); got.Role != "" || got.IsSuperuser || got.ImpersonationActive {
		t.Fatalf("the envelope's elevated claims reached the pipeline: %+v", got)
	}
	status, reason, _ := request(func(r *http.Request) {
		r.Header.Set(internalidentity.HeaderOrgID, org)
		r.Header.Set(internalidentity.HeaderRole, "member")
		r.Header.Set(internalidentity.HeaderSuperuser, "false")
		r.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	})
	if status != http.StatusUnauthorized || reason != mcpReasonProofCarrier {
		t.Fatalf("identity headers beside a valid envelope: status %d reason %q, want 401 %s", status, reason, mcpReasonProofCarrier)
	}
	if status, reason, _ := request(func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) }); status != http.StatusUnauthorized || reason != mcpReasonProofCarrier {
		t.Fatalf("two Authorization values: status %d reason %q, want 401 %s", status, reason, mcpReasonProofCarrier)
	}
	if status, reason, _ := request(func(r *http.Request) { r.Header.Set("Authorization", "Basic "+token) }); status != http.StatusUnauthorized || reason != mcpReasonProofCarrier {
		t.Fatalf("a non-bearer carrier: status %d reason %q, want 401", status, reason)
	}
}
