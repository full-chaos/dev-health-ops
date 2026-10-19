package local

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PGAttempts is Attempts over auth.login_attempts.
type PGAttempts struct{}

// NewPGAttempts builds the PostgreSQL lockout store.
func NewPGAttempts(*pgxpool.Pool, authschema.ValidatedIdentifier) *PGAttempts { return &PGAttempts{} }

func (*PGAttempts) Check(context.Context, []byte) (bool, time.Duration, error) {
	return false, 0, errNotImplemented
}
func (*PGAttempts) RecordFailure(context.Context, []byte) error { return errNotImplemented }
func (*PGAttempts) Clear(context.Context, []byte) error         { return errNotImplemented }
