// Package publicread is the read side of the versioned read API for
// organizations, memberships and user principals. Its record is go-api's
// public.users, public.organizations and public.memberships: it reads no
// auth-schema copy of them. It reads the auth schema for three things only:
// the aggregate revision counter, and the principals of the service account and
// workload kinds, which have no public row.
package publicread

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"
)

var (
	// ErrNotFound is a row that does not exist (or is not served).
	ErrNotFound = errors.New("publicread: not found")
	// ErrUnknownRole is a public membership role outside the closed catalog.
	ErrUnknownRole = errors.New("publicread: membership role outside the catalog")
	// ErrStore is a failed read.
	ErrStore = errors.New("publicread: store unavailable")
)

// Querier is the pool or connection a Reader reads through.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Organization is one public.organizations row with its revision (0 = none).
type Organization struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Active    bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

// Membership is one active public.memberships row with its revision (0 = none).
type Membership struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	JoinedAt    *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Revision    int64
}

// OrganizationOf pairs an organization with the membership of the principal asked about.
type OrganizationOf struct {
	Organization Organization
	Membership   Membership
}

// Principal is a principal's id, kind, status and revision, and nothing else.
type Principal struct {
	ID       uuid.UUID
	Kind     string
	Active   bool
	Revision int64
}

// Reader reads the public tables.
type Reader struct {
	q        Querier
	auth     string
	logger   *slog.Logger
	badRoles metric.Int64Counter
}

// Config builds a Reader. AuthSchema is the quoted auth schema name.
type Config struct {
	Querier    Querier
	AuthSchema string
	Logger     *slog.Logger
	Meter      metric.Meter
}

// New returns a Reader.
func New(cfg Config) (*Reader, error) {
	return &Reader{q: cfg.Querier, auth: cfg.AuthSchema, logger: cfg.Logger}, nil
}

var errNotImplemented = errors.New("publicread: not implemented")

// Statements are the reader's SQL texts by name, for the schema-qualified census.
func Statements(authSchema string) map[string]string { return map[string]string{} }

func (r *Reader) Organization(ctx context.Context, id uuid.UUID) (Organization, error) {
	return Organization{}, errNotImplemented
}

func (r *Reader) OrganizationsOf(ctx context.Context, principal uuid.UUID, after *uuid.UUID, limit int) ([]OrganizationOf, bool, error) {
	return nil, false, errNotImplemented
}

func (r *Reader) Membership(ctx context.Context, org, principal uuid.UUID) (Membership, error) {
	return Membership{}, errNotImplemented
}

func (r *Reader) Memberships(ctx context.Context, org uuid.UUID, after *uuid.UUID, limit int) ([]Membership, bool, error) {
	return nil, false, errNotImplemented
}

func (r *Reader) Principal(ctx context.Context, id uuid.UUID) (Principal, error) {
	return Principal{}, errNotImplemented
}

func (r *Reader) Principals(ctx context.Context, org uuid.UUID, kind string, after *uuid.UUID, limit int) ([]Principal, bool, error) {
	return nil, false, errNotImplemented
}
