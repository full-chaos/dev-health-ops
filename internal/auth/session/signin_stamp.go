package session

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

type signInKey struct{}

// WithSignIn marks ctx as a sign-in's: CreateAudited then stamps the user's
// last login (see Store.stampSignIn). Other session creators never stamp.
func WithSignIn(ctx context.Context) context.Context {
	return context.WithValue(ctx, signInKey{}, true)
}

// IsSignIn reports whether ctx came through WithSignIn.
func IsSignIn(ctx context.Context) bool {
	on, _ := ctx.Value(signInKey{}).(bool)
	return on
}

// stampSignIn runs in CreateAudited's transaction right after the session
// insert (err is that insert's result). For a sign-in under users owned it
// writes the session's issued_at as public.users.last_login_at and
// updated_at, the pair go-api's touchLastLogin writes while go-api owns
// users; a user with no public.users row fails the transaction. Any other
// call, or users not owned, writes nothing.
func (s *Store) stampSignIn(ctx context.Context, tx audit.TxOps, sessionID uuid.UUID, err error) error {
	if err != nil || !s.usersOwned || !IsSignIn(ctx) {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE public.users u SET last_login_at = s.issued_at, updated_at = s.issued_at
		FROM `+s.q+`.sessions s WHERE s.id = $1 AND u.id = s.principal_id`, sessionID.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("session: the sign-in's user has no public.users row")
	}
	return nil
}
