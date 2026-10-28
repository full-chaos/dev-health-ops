package membership

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// OwnsUsers is AUTH_USERS_OWNED. Off, go-api writes memberships and every
	// verb refuses with ErrNotOwned.
	OwnsUsers bool
	Logger    *slog.Logger
	Meter     metric.Meter
}

// Locked is what Authorize sees: the rows the verb locked, read in its
// transaction.
type Locked struct {
	OrgID      uuid.UUID
	Principals []uuid.UUID
}

// Authorize decides, inside the verb's transaction and after its locks, whether
// the actor may make the change. A nil Authorize refuses (ErrDenied).
type Authorize func(ctx context.Context, tx audit.TxOps, l Locked) error

// Change names one membership change.
type Change struct {
	OrgID            uuid.UUID
	PrincipalID      uuid.UUID
	Role             string
	InvitedBy        *uuid.UUID
	ExpectedRevision *int64
	Actor            Actor
	Authorize        Authorize
}

// Transfer moves the ownership of an organization from one member to another.
type Transfer struct {
	OrgID     uuid.UUID
	From, To  uuid.UUID
	Actor     Actor
	Authorize Authorize
}

// Membership is one auth.memberships row with its revision.
type Membership struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        *string
	Status      Status
	JoinedAt    *time.Time
	Revision    int64
}

type Store struct {
	pool      *pgxpool.Pool
	q         string
	ownsUsers bool
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
}

func New(cfg Config) (*Store, error) {
	return nil, errors.New("membership: not built")
}

func (s *Store) Add(ctx context.Context, c Change) (*Membership, error) { return nil, ErrStore }

func (s *Store) ChangeRole(ctx context.Context, c Change) (*Membership, error) { return nil, ErrStore }

func (s *Store) Suspend(ctx context.Context, c Change) (*Membership, error) { return nil, ErrStore }

func (s *Store) Reactivate(ctx context.Context, c Change) (*Membership, error) { return nil, ErrStore }

func (s *Store) Remove(ctx context.Context, c Change) (*Membership, error) { return nil, ErrStore }

func (s *Store) TransferOwnership(ctx context.Context, t Transfer) error { return ErrStore }

// AddInTx is Add inside a caller's audit.Commit (an invite accept). It returns
// the membership's provisioning event for the caller's mutation; the caller's
// audit row names the change.
func (s *Store) AddInTx(ctx context.Context, tx audit.TxOps, c Change) (*Membership, audit.OutboxEvent, error) {
	return nil, audit.OutboxEvent{}, ErrStore
}

func (s *Store) Get(ctx context.Context, orgID, principalID uuid.UUID) (*Membership, error) {
	return nil, ErrStore
}
