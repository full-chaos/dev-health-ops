package authapi

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// EventLoginSucceeded is a sign-in that started a session.
const EventLoginSucceeded = "auth.login.succeeded"

const reasonNotMember = "denied_no_membership"

func (h handlers) compatLogin(w http.ResponseWriter, _ *http.Request) { notBuilt(w) }

type loginSessions struct{ h handlers }

func (s loginSessions) Create(context.Context, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	return session.Issued{}, nil
}
