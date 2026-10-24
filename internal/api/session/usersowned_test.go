package session

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// Every go-api route that writes public.users answers the moved operation
// while auth-service owns users, before it opens a transaction or reads the
// request: the handlers here have no Pool, so any write or read would panic.
func TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")
	h := handlers{}
	for name, serve := range map[string]http.HandlerFunc{
		"change-password": h.changePassword,
		"reset-password":  h.resetPassword,
		"verify":          h.verify,
		"register":        h.register,
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			serve(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+name, nil))
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"operation_moved"`) {
				t.Fatalf("%s: status %d body %q, want 409 operation_moved", name, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestUsersOwnedRefusesTheSocialAccountCreate(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")
	h := handlers{}
	if _, err := h.createSocialUser(nil, nil, nil, "a@example.test", "github", "1"); !errors.Is(err, usersowned.ErrRefused) {
		t.Fatalf("createSocialUser = %v, want usersowned.ErrRefused before any write", err)
	}
}
