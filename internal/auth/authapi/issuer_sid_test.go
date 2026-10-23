package authapi

import (
	"testing"

	"github.com/google/uuid"
)

// Every access token auth-service mints carries its session's sid: go-api and
// query-api end the token when that session is revoked, so a token without a
// real session must never be minted.
func TestIssuerAccessAlwaysCarriesItsSession(t *testing.T) {
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	principal, sessionID := uuid.New(), uuid.New()
	token, err := issuer.Access(Grant{PrincipalID: principal, SessionID: sessionID, Role: "member"})
	if err != nil {
		t.Fatal(err)
	}
	bearer, _, err := issuer.Verify(token)
	if err != nil {
		t.Fatalf("the minted token does not verify: %v", err)
	}
	if bearer.SessionID != sessionID || bearer.PrincipalID != principal {
		t.Fatalf("token names session %s of %s, want %s of %s", bearer.SessionID, bearer.PrincipalID, sessionID, principal)
	}
	if token, err := issuer.Access(Grant{PrincipalID: principal, Role: "member"}); err == nil || token != "" {
		t.Fatalf("a grant without a session minted a token (%d bytes, err %v); want none", len(token), err)
	}
	if token, err := issuer.Access(Grant{SessionID: sessionID, Role: "member"}); err == nil || token != "" {
		t.Fatalf("a grant without a principal minted a token (%d bytes, err %v); want none", len(token), err)
	}
}
