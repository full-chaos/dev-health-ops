package authapi

import (
	"net/http"
	"testing"
)

// A go-api refresh JWT ends nothing at logout, even one the legacy key verifies:
// auth-service no longer touches public.refresh_tokens.
func TestCompatLogoutVerifiedLegacyTokenRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	logouts := installLogoutCounter(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(legacyToken(t, f, f.org.String(), testSecret)), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.revokedLegacy) != 0 || len(f.sessions.byCredential) != 0 {
		t.Fatalf("revoked lineages %v credentials %v, want none", f.sessions.revokedLegacy, f.sessions.byCredential)
	}
	if got := legacyLogoutTotal(t, logouts); got != 0 {
		t.Fatalf("logout counter %d, want 0", got)
	}
}
