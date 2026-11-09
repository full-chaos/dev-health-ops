package authapi

import (
	"net/http"
	"testing"
)

// A go-api refresh JWT ends nothing at logout, even one the legacy key verifies:
// auth-service no longer touches public.refresh_tokens.
func TestCompatLogoutVerifiedLegacyTokenRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(legacyToken(t, f, f.org.String(), testSecret)), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 0 || len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked credentials %v sessions %v, want none", f.sessions.byCredential, f.sessions.revoked)
	}
}
