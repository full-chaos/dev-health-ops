package session

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// go-api serves no local body on the five credential link routes: with the
// users-owned switch off or on they answer the moved-operation refusal, and
// auth-service answers them through the credentials forward. The handlers
// here have no Pool and no mail config, so any read, write or mail would
// panic or answer something else.
func TestCredentialLinkRoutesAnswerTheOwnedAnswerWhileNotOwned(t *testing.T) {
	want := httptest.NewRecorder()
	usersowned.Write(want)
	for _, env := range []string{"", "false", "true"} {
		t.Setenv(usersowned.Env, env)
		h := handlers{}
		for name, serve := range map[string]http.HandlerFunc{
			"register":            h.register,
			"verify":              h.verify,
			"resend-verification": h.resendVerification,
			"forgot-password":     h.forgotPassword,
			"reset-password":      h.resetPassword,
		} {
			recorder := httptest.NewRecorder()
			serve(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+name, nil))
			if recorder.Code != want.Code || recorder.Body.String() != want.Body.String() ||
				recorder.Header().Get("Content-Type") != want.Header().Get("Content-Type") {
				t.Errorf("AUTH_USERS_OWNED=%q %s: status %d body %q, want %d %q", env, name, recorder.Code, recorder.Body.String(), want.Code, want.Body.String())
			}
		}
	}
}
