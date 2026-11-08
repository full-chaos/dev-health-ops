package authapi

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Every access token auth-service mints carries its session's sid: go-api and
// query-api end the token when that session is revoked, so a token without a
// real session must never be minted.
func TestIssuerAccessAlwaysCarriesItsSession(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, contracts.PlatformAPIAudience)
	principal, sessionID := uuid.New(), uuid.New()
	token, err := issuer.Access(Grant{PrincipalID: principal, SessionID: sessionID, Role: "member"})
	if err != nil || token == "" {
		t.Fatalf("a grant with its principal and session minted no token: %v", err)
	}
	if token, err := issuer.Access(Grant{PrincipalID: principal, Role: "member"}); err == nil || token != "" {
		t.Fatalf("a grant without a session minted a token (%d bytes, err %v); want none", len(token), err)
	}
	if token, err := issuer.Access(Grant{SessionID: sessionID, Role: "member"}); err == nil || token != "" {
		t.Fatalf("a grant without a principal minted a token (%d bytes, err %v); want none", len(token), err)
	}
}
