package authapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// dropLegacyKey leaves the fixture's routes as they stand once the legacy key
// is gone: the Issuer they hold has no HS256 verifier.
func dropLegacyKey(f *compatFixture) { f.issuer.verifier = nil }

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

func TestCompatLogoutWithoutLegacyKeyAnswers200AndRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	token := legacyToken(t, f, f.org.String(), testSecret)
	dropLegacyKey(f)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(token), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 0 {
		t.Fatalf("revoked credentials %v from a token no key verified", f.sessions.byCredential)
	}
}

func TestCompatLogoutWithoutLegacyKeyStillRevokesAnOpaqueCredential(t *testing.T) {
	f := newCompatFixture(t)
	dropLegacyKey(f)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody("opaque-credential"), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 1 || f.sessions.byCredential[0] != "opaque-credential" {
		t.Fatalf("revoked credentials %v, want the opaque one", f.sessions.byCredential)
	}
}

func TestVerifyLegacyRefreshWithoutLegacyKeyIsRefused(t *testing.T) {
	f := newCompatFixture(t)
	token := legacyToken(t, f, f.org.String(), testSecret)
	dropLegacyKey(f)
	legacy, err := f.issuer.VerifyLegacyRefresh(token)
	if !errors.Is(err, errLegacyKeyAbsent) || legacy.JTI.Reveal() != "" {
		t.Fatalf("VerifyLegacyRefresh = %+v, %v, want errLegacyKeyAbsent and no claims", legacy, err)
	}
}
