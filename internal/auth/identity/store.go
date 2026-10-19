// Package identity is the auth-service identity store: the principal
// projection of public.users, the login lookup by email, and external
// identities keyed by (issuer, provider, subject), on the auth schema.
//
// This is the red stub: the signatures the tests drive, with no behaviour.
package identity

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	ErrMultipleUsers  = errors.New("identity: more than one user matches the email")
	ErrUserNotFound   = errors.New("identity: user not found")
	ErrEmailCollision = errors.New("identity: the email belongs to another principal")
	ErrPrincipalKind  = errors.New("identity: the principal is not a user")
	ErrStore          = errors.New("identity: store unavailable")
	errNotImplemented = errors.New("identity: not implemented")
)

// Config builds a Store.
type Config struct {
	Pool             *pgxpool.Pool
	Schema           authschema.ValidatedIdentifier
	Logger           *slog.Logger
	UsersOwnedByAuth bool
	Meter            metric.Meter
}

// Store is the identity store over one auth schema.
type Store struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
}

// New builds a Store.
func New(cfg Config) (*Store, error) {
	return &Store{pool: cfg.Pool, schema: cfg.Schema}, nil
}

// Principal is one user principal.
type Principal struct {
	ID           uuid.UUID
	IsActive     bool
	PasswordHash *string
	IsVerified   bool
	AuthProvider *string
	DisabledAt   *time.Time
}

// ByEmail is a stub.
func (s *Store) ByEmail(ctx context.Context, normalizedEmail string) (*Principal, error) {
	return nil, errNotImplemented
}

// Project is a stub.
func (s *Store) Project(ctx context.Context, userID uuid.UUID) error { return errNotImplemented }

// ByID is a stub.
func (s *Store) ByID(ctx context.Context, id uuid.UUID) (*Principal, error) {
	return nil, errNotImplemented
}
