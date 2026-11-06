package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testLinkSecret is the link secret of a handler under test that must pass the config check.
const testLinkSecret = "unit-test-link-secret-0123456789abcdef"

// With no LINK_TOKEN_SECRET every go-api route that signs or accepts a link
// token answers 503 config_missing before it opens a transaction: the
// handlers here have no Pool, so any query would panic.
func TestLinkRoutesAnswer503WithoutLinkSecret(t *testing.T) {
	h := handlers{}
	for name, serve := range map[string]http.HandlerFunc{
		"register":            h.register,
		"verify":              h.verify,
		"resend-verification": h.resendVerification,
		"forgot-password":     h.forgotPassword,
		"reset-password":      h.resetPassword,
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			serve(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+name, nil))
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "config_missing: LINK_TOKEN_SECRET") {
				t.Fatalf("%s: status %d body %q, want 503 config_missing", name, recorder.Code, recorder.Body.String())
			}
		})
	}
	t.Run("invite join", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		_, _, ok := h.joinByInvite(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/accept-invite", nil), nil, nil, "token", "Invite accepted", time.Now())
		if ok || recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "config_missing: LINK_TOKEN_SECRET") {
			t.Fatalf("joinByInvite ok=%v status %d body %q, want 503 config_missing", ok, recorder.Code, recorder.Body.String())
		}
	})
}
