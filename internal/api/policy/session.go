package policy

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// SessionRow is the auth-service session an access token's sid names.
type SessionRow struct {
	PrincipalID uuid.UUID
	Revoked     bool
}

// SessionStore is the Store half that reads auth-service sessions: a token
// auth-service issued carries sid and is good only while that session lives.
type SessionStore interface {
	SessionState(ctx context.Context, id uuid.UUID) (row SessionRow, found bool, err error)
}

// SessionState reads the session id names.
func (s PGStore) SessionState(context.Context, uuid.UUID) (SessionRow, bool, error) {
	return SessionRow{}, false, errors.New("policy: session reads are not wired")
}
