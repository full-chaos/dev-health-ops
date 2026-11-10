package authapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func logoutBody(token string) string { return `{"refresh_token":"` + token + `"}` }

func assertLogoutDone(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s, want 200", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), textLogoutDone) {
		t.Fatalf("body %s, want the logout message", response.Body)
	}
}

func TestCompatLogoutKeySetForeignKeyTokenRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(legacyToken(t, f, f.org.String(), strings.Repeat("k", len(testSecret)+1))), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 0 {
		t.Fatalf("revoked credentials %v from a token another key signed", f.sessions.byCredential)
	}
}

// A go-api refresh token is verified by nothing: logout answers 200 and revokes nothing.
func TestCompatLogoutOfAGoAPIRefreshTokenAnswers200AndRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	token := legacyToken(t, f, f.org.String(), testSecret)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(token), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 0 {
		t.Fatalf("revoked credentials %v from a token no key verified", f.sessions.byCredential)
	}
}

func TestCompatLogoutRevokesAnOpaqueCredential(t *testing.T) {
	f := newCompatFixture(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody("opaque-credential"), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 1 || f.sessions.byCredential[0] != "opaque-credential" {
		t.Fatalf("revoked credentials %v, want the opaque one", f.sessions.byCredential)
	}
}
