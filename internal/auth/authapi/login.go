package authapi

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// EventLoginSucceeded is a sign-in that started a session.
const EventLoginSucceeded = "auth.login.succeeded"

const reasonNotMember = "denied_no_membership"

type loginSessions struct{ h compatHandlers }

func (s loginSessions) Create(context.Context, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	return session.Issued{}, nil
}
