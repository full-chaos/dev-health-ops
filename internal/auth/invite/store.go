package invite

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// OwnsUsers is AUTH_USERS_OWNED. Off, go-api writes invites and every verb
	// refuses with ErrNotOwned.
	OwnsUsers bool
	// Memberships writes the membership an accept creates, in the accept's commit.
	Memberships *membership.Store
	// Secret signs new tokens; AcceptSecrets verify them (the current secret
	// first). They are go-api's invite secrets, so a token either side minted
	// is accepted by both.
	Secret        string
	AcceptSecrets []string
	// Now is injectable for tests; nil is time.Now.
	Now    func() time.Time
	Logger *slog.Logger
	Meter  metric.Meter
}

// Authorize decides, inside the verb's transaction, whether the actor may
// create or revoke an invite of the organization. A nil Authorize refuses.
type Authorize func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error

// PublicAudit is the request part of go-api's audit_logs row the verb writes
// in its commit (the one writer of that row while auth-service owns users).
type PublicAudit struct {
	Description     *string
	RequestMetadata []byte
}

type CreateInput struct {
	OrgID       uuid.UUID
	Email       string
	Role        string
	TTL         *time.Duration
	InvitedBy   uuid.UUID
	Actor       Actor
	Authorize   Authorize
	PublicAudit *PublicAudit
}

// Created is a new invite. Token is shown once and never stored.
type Created struct {
	ID        uuid.UUID
	Token     string
	ExpiresAt time.Time
}

type AcceptInput struct {
	Token       string
	PrincipalID uuid.UUID
	Actor       Actor
	PublicAudit *PublicAudit
}

type Accepted struct {
	InviteID     uuid.UUID
	OrgID        uuid.UUID
	MembershipID uuid.UUID
	Role         string
}

type RevokeInput struct {
	OrgID     uuid.UUID
	InviteID  uuid.UUID
	Actor     Actor
	Authorize Authorize
}

type Store struct {
	pool *pgxpool.Pool
}

func New(cfg Config) (*Store, error) { return nil, errors.New("invite: not built") }

func (s *Store) Create(ctx context.Context, in CreateInput) (*Created, error) { return nil, ErrStore }

func (s *Store) Accept(ctx context.Context, in AcceptInput) (*Accepted, error) { return nil, ErrStore }

func (s *Store) Revoke(ctx context.Context, in RevokeInput) error { return ErrStore }
