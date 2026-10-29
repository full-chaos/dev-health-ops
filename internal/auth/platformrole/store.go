package platformrole

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// activeSQL is the current-state rule of an assignment, on the database clock:
// a user principal that is not disabled, and an assignment that has not
// expired. r is the assignment row, p its principal.
var _ = activeSQL

const activeSQL = `(p.kind = 'user' AND p.disabled_at IS NULL AND (r.expires_at IS NULL OR r.expires_at > now()))`

// Config builds a Store.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Logger *slog.Logger
}

// Store reads and writes auth.platform_role_assignments.
type Store struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
	q      string
	logger *slog.Logger
}

// New validates the configuration and builds a Store.
func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("platformrole: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("platformrole: no auth schema")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{pool: cfg.Pool, schema: cfg.Schema, q: authschema.Quote(cfg.Schema), logger: logger}, nil
}

// Assignment is one stored platform role assignment and whether it is active
// now.
type Assignment struct {
	ID          uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	GrantedBy   *uuid.UUID
	ExpiresAt   *time.Time
	Active      bool
}

// GrantInput is one grant. Actor must hold GrantAction now.
type GrantInput struct {
	Actor     uuid.UUID
	Principal uuid.UUID
	Role      string
	// ExpiresAt is nil for a permanent assignment.
	ExpiresAt *time.Time
}

// RevokeInput is one revoke. Actor must hold GrantAction now.
type RevokeInput struct {
	Actor     uuid.UUID
	Principal uuid.UUID
	Role      string
}

// Resolve is the platform authority principal holds now.
func (s *Store) Resolve(ctx context.Context, principal uuid.UUID) (Authority, error) {
	return Authority{}, nil
}

// Grant gives in.Principal the platform role in.Role.
func (s *Store) Grant(ctx context.Context, in GrantInput) (uuid.UUID, error) {
	return uuid.New(), nil
}

// Revoke removes in.Role from in.Principal.
func (s *Store) Revoke(ctx context.Context, in RevokeInput) error {
	return nil
}

// List is every stored assignment.
func (s *Store) List(ctx context.Context) ([]Assignment, error) {
	return nil, nil
}

var refusals = []error{ErrGranterNotHeld, ErrPrincipalMissing, ErrNotHumanUser, ErrPrincipalOff,
	ErrExpiryNotFuture, ErrAlreadyHeld, ErrNotHeld, ErrLastPlatformAdmin}

// writeErr returns a refusal as itself and any other failure as ErrStore.
func (s *Store) writeErr(ctx context.Context, op string, err error) error {
	return err
}

func (s *Store) storeErr(ctx context.Context, op string, err error) error {
	s.logger.ErrorContext(ctx, "platformrole: store failure", "op", op, logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, op, err)
}
