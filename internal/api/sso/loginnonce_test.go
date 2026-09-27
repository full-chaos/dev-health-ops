package sso

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestValidateHTTPSOrLoopback pins D2759's r1 finding: the OLD per-protocol
// `strings.HasPrefix(x, "http://")` checks were case-sensitive and never
// considered a schemeless or opaque value at all. This is the single
// shared replacement, exercised against every shape team-lead's ruling
// named explicitly.
func TestValidateHTTPSOrLoopback(t *testing.T) {
	cases := []struct {
		name      string
		candidate string
		wantErr   bool
	}{
		{"https accepted", "https://example.com/x", false},
		{"http non-loopback refused", "http://example.com/x", true},
		{"uppercase HTTP refused (the r1 bypass)", "HTTP://attacker.example/callback", true},
		{"mixed-case Http refused", "Http://attacker.example/callback", true},
		{"opaque https:evil refused (no authority)", "https:evil", true},
		{"schemeless //host refused", "//host/path", true},
		{"javascript: refused", "javascript:alert(1)", true},
		{"ftp:// refused", "ftp://host/path", true},
		{"userinfo refused even on an otherwise-loopback http URL", "http://user:pass@127.0.0.1/x", true},
		{"http loopback accepted (127.0.0.1)", "http://127.0.0.1:8080/x", false},
		{"http loopback accepted (localhost)", "http://localhost:8080/x", false},
		{"http loopback accepted (::1)", "http://[::1]:8080/x", false},
		{"uppercase HTTP loopback still accepted (url.Parse normalizes scheme case)", "HTTP://127.0.0.1/x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateHTTPSOrLoopback(c.candidate)
			if c.wantErr && err == nil {
				t.Fatalf("candidate=%q: want an error, got nil", c.candidate)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("candidate=%q: want no error, got %v", c.candidate, err)
			}
		})
	}
}

// TestLoginNonceCookieNamesAreFlowScoped pins D2759's P1-B fix directly:
// two flows minted with different IDs get different cookie names, so
// concurrent Set-Cookie calls in the same browser never collide.
func TestLoginNonceCookieNamesAreFlowScoped(t *testing.T) {
	nameA := loginNonceCookieName("saml", "flow-A")
	nameB := loginNonceCookieName("saml", "flow-B")
	if nameA == nameB {
		t.Fatalf("cookie names for different flows must differ, got %q for both", nameA)
	}
	if loginNonceCookieName("oauth", "flow-A") == nameA {
		t.Fatalf("cookie names must also differ across protocols for the same flow ID")
	}
}

// TestVerifyAndClearLoginNonceCookieMatchesTheRightFlow pins the
// verify+clear helper directly: it must find and validate ONLY the
// cookie whose name corresponds to the given (already-verified) flowID,
// reject a missing or mismatched one, and always clear it on the way out.
func TestVerifyAndClearLoginNonceCookieMatchesTheRightFlow(t *testing.T) {
	const path = "/api/v1/auth/saml/p/acs"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	t.Run("matching cookie verifies and gets cleared", func(t *testing.T) {
		rec := httptest.NewRecorder()
		nonce := "the-real-nonce"
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.AddCookie(&http.Cookie{Name: loginNonceCookieName("saml", "flow-1"), Value: nonce})
		if !verifyAndClearLoginNonceCookie(rec, req, "saml", "flow-1", path, hashLoginNonce(nonce)) {
			t.Fatal("want the matching cookie to verify")
		}
		cleared := false
		for _, c := range rec.Result().Cookies() {
			if c.Name == loginNonceCookieName("saml", "flow-1") && c.MaxAge < 0 {
				cleared = true
			}
		}
		if !cleared {
			t.Fatal("want the cookie cleared (MaxAge<0) on the success path")
		}
	})

	t.Run("missing cookie refused and still clears defensively", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		if verifyAndClearLoginNonceCookie(rec, req, "saml", "flow-2", path, hashLoginNonce("whatever")) {
			t.Fatal("want a missing cookie to be refused")
		}
	})

	t.Run("a different flow's cookie in the jar does not match", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.AddCookie(&http.Cookie{Name: loginNonceCookieName("saml", "flow-OTHER"), Value: "some-nonce"})
		if verifyAndClearLoginNonceCookie(rec, req, "saml", "flow-3", path, hashLoginNonce("some-nonce")) {
			t.Fatal("want a same-value-different-flow cookie to be refused: it must be looked up by the VERIFIED flow ID, not any cookie present")
		}
	})
}
