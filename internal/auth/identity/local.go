package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidPasswordHash reports a password verifier that is not a bcrypt
// hash.
var ErrInvalidPasswordHash = errors.New("identity: the password hash is not a bcrypt hash")

// LocalProvisionRequest is a new local (email and password) user.
type LocalProvisionRequest struct {
	Email        string
	FullName     string
	PasswordHash *string
	Actor        Actor
}

// ProvisionLocal is a stub.
func (s *Store) ProvisionLocal(ctx context.Context, req LocalProvisionRequest) (uuid.UUID, error) {
	return uuid.Nil, errNotImplemented
}

// SetEmailVerified is a stub.
func (s *Store) SetEmailVerified(ctx context.Context, principal uuid.UUID, at time.Time, actor Actor) error {
	return errNotImplemented
}

// SetPasswordHash is a stub.
func (s *Store) SetPasswordHash(ctx context.Context, principal uuid.UUID, hash string, actor Actor) error {
	return errNotImplemented
}
