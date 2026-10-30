package org

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const organizationsSlugKey = "organizations_slug_key"

type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Logger *slog.Logger
	Meter  metric.Meter
}

type CreateInput struct {
	Name string
	Slug string
}

type Store struct {
	pool      *pgxpool.Pool
	q         string
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
}

func New(cfg Config) (*Store, error) {
	if cfg.Pool == nil {
		return nil, errors.New("org: no connection pool")
	}
	if cfg.Schema.String() == "" {
		return nil, errors.New("org: no auth schema")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), logger: cfg.Logger}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/org")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.org.mutations",
		metric.WithDescription("Organization mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("org: registering the mutation counter: %w", err)
	}
	return s, nil
}

func (s *Store) record(ctx context.Context, v verb, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", string(v)), attribute.String("outcome", outcomeLabel(err))))
}

func (s *Store) run(ctx context.Context, v verb, m audit.Mutation) error {
	_, err := s.runChanged(ctx, v, m)
	return err
}

// runChanged is run that also says whether the commit changed state: a no-op
// (audit.ErrNoStateChange) is success with changed false.
func (s *Store) runChanged(ctx context.Context, v verb, m audit.Mutation) (bool, error) {
	err := s.commit(ctx, m)
	s.record(ctx, v, err)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return false, nil
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrSlugTaken), errors.Is(err, ErrRevisionConflict), errors.Is(err, ErrSuspended),
		errors.Is(err, ErrDenied):
		return false, err
	}
	s.logger.ErrorContext(ctx, "org: store failure", "verb", string(v), logging.ErrorAttr(err))
	return false, fmt.Errorf("%w: %s: %w", ErrStore, v, err)
}

// Create inserts an active organization with a server-minted id. The slug is
// required, validated against the wire pattern, and immutable afterwards.
func (s *Store) Create(ctx context.Context, in CreateInput, a Actor) (uuid.UUID, error) {
	name, err := NormalizeName(in.Name)
	if err == nil {
		err = ValidateSlug(in.Slug)
	}
	if err != nil {
		s.record(ctx, verbCreate, err)
		return uuid.Nil, err
	}
	id := uuid.New()
	err = s.run(ctx, verbCreate, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.organizations (id, slug, name) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING RETURNING id`, id, in.Slug, name).Scan(&inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrSlugTaken
			}
			return err
		},
		Audit:  auditFor(verbCreate, id, a, "", StatusActive),
		Events: []audit.OutboxEvent{eventFor(verbCreate, id, in.Slug)},
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// Rename changes the display name of an active organization.
func (s *Store) Rename(ctx context.Context, id uuid.UUID, name string, ifRevision *int64, a Actor) error {
	n, err := NormalizeName(name)
	if err != nil {
		s.record(ctx, verbRename, err)
		return err
	}
	_, err = s.mutate(ctx, verbRename, id, n, ifRevision, a, nil)
	return err
}

// Suspend sets disabled_at. A repeat is a no-op.
func (s *Store) Suspend(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor) error {
	_, err := s.mutate(ctx, verbSuspend, id, "", ifRevision, a, nil)
	return err
}

// Reactivate clears disabled_at. A repeat is a no-op.
func (s *Store) Reactivate(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor) error {
	_, err := s.mutate(ctx, verbReactivate, id, "", ifRevision, a, nil)
	return err
}

// Authorize decides, inside the commit and after the organization row lock,
// whether the actor may make the change. ErrDenied refuses it; any other error
// is a store failure.
type Authorize func(ctx context.Context, tx audit.TxOps, id uuid.UUID) error

// ReactivateAuthorized is Reactivate for a caller that must authorize the
// actor under the row lock (D6456, deny first): authz runs after the lock and
// before any write, and a nil authz is ErrDenied before anything is planned.
// changed is false for an organization that was not suspended.
func (s *Store) ReactivateAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor, authz Authorize) (changed bool, err error) {
	if authz == nil {
		s.record(ctx, verbReactivate, ErrDenied)
		return false, ErrDenied
	}
	return s.mutate(ctx, verbReactivate, id, "", ifRevision, a, authz)
}

// SuspendAuthorized is Suspend for a caller that must authorize the actor under
// the row lock, exactly as ReactivateAuthorized: authz runs after the lock and
// before any write, and a nil authz is ErrDenied before anything is planned.
// changed is false for an organization that was already suspended.
func (s *Store) SuspendAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor, authz Authorize) (changed bool, err error) {
	if authz == nil {
		s.record(ctx, verbSuspend, ErrDenied)
		return false, ErrDenied
	}
	return s.mutate(ctx, verbSuspend, id, "", ifRevision, a, authz)
}

func (s *Store) mutate(ctx context.Context, v verb, id uuid.UUID, newName string, ifRevision *int64, a Actor, authz Authorize) (bool, error) {
	prev, next := StatusActive, StatusActive
	switch v {
	case verbSuspend:
		next = StatusSuspended
	case verbReactivate:
		prev = StatusSuspended
	}
	return s.runChanged(ctx, v, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var cur state
			// The row lock serialises verbs on one organization, so the
			// revision read below is the one Commit will bump.
			err := tx.QueryRow(ctx, `SELECT name, disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR UPDATE`, id).
				Scan(&cur.name, &cur.active)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			if authz != nil {
				if err := authz(ctx, tx, id); err != nil {
					return err
				}
			}
			if ifRevision != nil {
				var rev int64
				err := tx.QueryRow(ctx, `SELECT revision FROM `+s.q+`.aggregate_revisions WHERE aggregate_type = 'organization' AND aggregate_id = $1`,
					provisioning.WireID("organization", id.String())).Scan(&rev)
				if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
				if err := checkRevision(rev, ifRevision); err != nil {
					return err
				}
			}
			d, err := decide(v, cur, newName)
			if err != nil || d.noop {
				return err
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.organizations SET name = $2,
disabled_at = CASE WHEN $3::boolean THEN NULL ELSE COALESCE(disabled_at, clock_timestamp()) END,
updated_at = clock_timestamp() WHERE id = $1`, id, d.next.name, d.next.active)
			return err
		},
		Audit:  auditFor(v, id, a, prev, next),
		Events: []audit.OutboxEvent{eventFor(v, id, "")},
	})
}

func statusOf(active bool) Status {
	if active {
		return StatusActive
	}
	return StatusSuspended
}

const readOrg = `SELECT o.id, o.slug, o.name, o.disabled_at, o.created_at, o.updated_at, COALESCE(r.revision, 0)
FROM %s.organizations o LEFT JOIN %s.aggregate_revisions r
  ON r.aggregate_type = 'organization' AND r.aggregate_id = 'org_' || o.id::text `

func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Organization, error) {
	return s.read(ctx, `WHERE o.id = $1`, id)
}

func (s *Store) BySlug(ctx context.Context, slug string) (*Organization, error) {
	return s.read(ctx, `WHERE o.slug = $1`, slug)
}

func (s *Store) read(ctx context.Context, where string, arg any) (*Organization, error) {
	var o Organization
	err := s.pool.QueryRow(ctx, fmt.Sprintf(readOrg, s.q, s.q)+where, arg).
		Scan(&o.ID, &o.Slug, &o.Name, &o.SuspendedAt, &o.CreatedAt, &o.UpdatedAt, &o.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		s.logger.ErrorContext(ctx, "org: read failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	o.Status = statusOf(o.SuspendedAt == nil)
	return &o, nil
}
