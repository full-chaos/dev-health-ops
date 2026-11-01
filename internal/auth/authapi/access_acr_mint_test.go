package authapi

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type noImpersonation struct{}

func (noImpersonation) ImpersonatingActor(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}

// TestAnACRMintedTokenSignedByAuthServicesOwnKeyIsRefusedAtTheSessionRoutes is
// the O1 auth-service cell fed by the real producer (CHAOS-12250): acrmint
// signs with the very signer and issuer the session routes trust, and its
// token is still a 401 at every session route, before any session read or
// revoke. Positive controls: the session token of the same signer is served,
// and the minted token is a valid access-token.v1 document.
func TestAnACRMintedTokenSignedByAuthServicesOwnKeyIsRefusedAtTheSessionRoutes(t *testing.T) {
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/sessions/current"},
		{http.MethodPost, "/v1/sessions/revoke"},
		{http.MethodPost, "/v1/sessions/revoke-all"},
	}
	for _, audience := range []string{contracts.ACRAPIAudience, contracts.ACRMCPAudience} {
		t.Run(audience, func(t *testing.T) {
			a := newAccessFixture(t)
			if r := a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(a.claims())); r.Code != http.StatusOK {
				t.Fatalf("positive control: current = %d, body %s", r.Code, r.Body)
			}
			minter, err := acrmint.New(acrmint.Config{Signer: a.signer, IssuerURL: goldenIssuer, Impersonation: noImpersonation{}, Now: time.Now})
			if err != nil {
				t.Fatal(err)
			}
			minted, _, err := minter.Mint(context.Background(), acrmint.Request{Audience: audience, PrincipalID: a.principal, SessionID: a.record.ID, OrganizationID: a.org})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := contracts.ParseAccessToken(repoRoot(t), compactDocumentOf(t, minted)); err != nil {
				t.Fatalf("the minted %s token is not a valid access-token.v1 document: %v", audience, err)
			}
			a.sessions.currentErr = errors.New("a session read")
			code, reason := acrRefusal(t)
			for _, route := range routes {
				expectError(t, a.do(route.method, route.path, "", map[string]string{"Authorization": "Bearer " + minted}), http.StatusUnauthorized, code)
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

// compactDocumentOf is the {"header", "claims"} document of a compact JWS.
func compactDocumentOf(t *testing.T, compact string) []byte {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("%d parts", len(parts))
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	claims, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return []byte(`{"header":` + string(header) + `,"claims":` + string(claims) + `}`)
}
