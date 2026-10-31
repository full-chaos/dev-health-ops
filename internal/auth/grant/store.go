package grant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/grantend"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Config builds a Store.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// OwnsUsers is AUTH_USERS_OWNED. Off, every write verb refuses with
	// ErrNotOwned before it opens a transaction: the pair locks need UPDATE on
	// public.users and public.memberships, which the runtime holds only then.
	OwnsUsers bool
	// Members locks and reads the (organization, principal) pair.
	Members *membership.Store
	// Catalog names the actions of a role for the grant events; nil is the
	// embedded catalog.
	Catalog *policycatalog.Catalog
	Logger  *slog.Logger
	Meter   metric.Meter
}

// Store is the resource grant aggregate.
type Store struct {
	pool      *pgxpool.Pool
	q         string
	ownsUsers bool
	members   *membership.Store
	catalog   *policycatalog.Catalog
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
}

// New builds a Store.
func New(cfg Config) (*Store, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("grant: no connection pool")
	case cfg.Schema.String() == "":
		return nil, errors.New("grant: no auth schema")
	case cfg.Members == nil:
		return nil, errors.New("grant: no membership store")
	}
	s := &Store{pool: cfg.Pool, q: authschema.Quote(cfg.Schema), ownsUsers: cfg.OwnsUsers, members: cfg.Members, catalog: cfg.Catalog, logger: cfg.Logger}
	s.commit = func(ctx context.Context, m audit.Mutation) error { return audit.Commit(ctx, cfg.Pool, cfg.Schema, m) }
	if s.catalog == nil {
		s.catalog = policycatalog.Default()
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/grant")
	}
	var err error
	if s.mutations, err = meter.Int64Counter("auth.grant.mutations",
		metric.WithDescription("Resource grant mutations, by verb and outcome.")); err != nil {
		return nil, fmt.Errorf("grant: registering the mutation counter: %w", err)
	}
	return s, nil
}

var errStateMoved = membership.ErrStateMoved

const planAttempts = 5

// refusals are the store's named outcomes, returned as they are.
var refusals = []error{ErrNotOwned, ErrDenied, ErrSuperuserRequired, ErrOrgNotFound, ErrOrgSuspended, ErrPrincipalNotFound,
	ErrRevisionConflict, ErrKeyInvalid, ErrRoleInvalid, ErrOriginInvalid, ErrActorRequired, ErrNotMember, ErrAlreadyGranted,
	ErrNotFound, ErrRevoked, ErrExpiryPast, ErrNothingToSet}

func outcomeLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, audit.ErrNoStateChange):
		return "noop"
	case errors.Is(err, ErrDenied), errors.Is(err, ErrSuperuserRequired):
		return "denied"
	case errors.Is(err, ErrNotOwned):
		return "not_owned"
	case errors.Is(err, ErrStore):
		return "store_error"
	}
	for _, r := range refusals {
		if errors.Is(err, r) {
			return "refused"
		}
	}
	return "store_error"
}

func (s *Store) record(ctx context.Context, verb string, err error) {
	s.mutations.Add(ctx, 1, metric.WithAttributes(attribute.String("verb", verb), attribute.String("outcome", outcomeLabel(err))))
}

func (s *Store) run(ctx context.Context, verb string, m audit.Mutation) (err error) {
	defer func() {
		s.record(ctx, verb, err)
		if errors.Is(err, audit.ErrNoStateChange) {
			err = nil
		}
	}()
	err = s.commit(ctx, m)
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	for _, r := range refusals {
		if errors.Is(err, r) {
			return r
		}
	}
	if errors.Is(err, errStateMoved) {
		return errStateMoved
	}
	s.logger.ErrorContext(ctx, "grant: store failure", "verb", verb, logging.ErrorAttr(err))
	return fmt.Errorf("%w: %s: %w", ErrStore, verb, err)
}

// refuse records and returns a refusal that needs no transaction.
func (s *Store) refuse(ctx context.Context, verb string, err error) error {
	s.record(ctx, verb, err)
	return err
}

func authorize(ctx context.Context, tx audit.TxOps, auth Authorize, l Locked) error {
	if auth == nil {
		return ErrDenied
	}
	if err := auth(ctx, tx, l); err != nil {
		switch {
		case errors.Is(err, ErrDenied):
			return ErrDenied
		case errors.Is(err, ErrSuperuserRequired):
			return ErrSuperuserRequired
		}
		return err
	}
	return nil
}

// lockPair locks the pair in the auth lock order and authorizes the actor on
// the locked rows. A missing row is named only to an actor Authorize accepts.
func (s *Store) lockPair(ctx context.Context, tx audit.TxOps, auth Authorize, orgID, principalID uuid.UUID, actor *uuid.UUID) (membership.PairState, error) {
	st, err := s.members.ReadPair(ctx, tx, orgID, principalID, actor)
	if err != nil {
		for _, lookup := range []error{ErrPrincipalNotFound, ErrOrgNotFound, ErrOrgSuspended} {
			if errors.Is(err, lookup) {
				if aerr := authorize(ctx, tx, auth, Locked{OrgID: orgID, Principals: []uuid.UUID{principalID}}); aerr != nil {
					return membership.PairState{}, aerr
				}
				return membership.PairState{}, lookup
			}
		}
		return membership.PairState{}, err
	}
	if err := authorize(ctx, tx, auth, Locked{OrgID: orgID, Principals: []uuid.UUID{principalID}}); err != nil {
		return membership.PairState{}, err
	}
	return st, nil
}

func (s *Store) actionSet(role string) []string {
	r, err := s.catalog.Role(role)
	if err != nil || len(r.Actions) == 0 || len(r.Actions) > 64 {
		return nil
	}
	return r.Actions
}

func (s *Store) createEvent(id uuid.UUID, in CreateInput) audit.OutboxEvent {
	change := map[string]any{"status": "active", "resource_type": KindRepository, "resource_key": in.Key}
	if set := s.actionSet(in.Role); set != nil {
		change["action_set"] = set
	}
	if in.ExpiresAt != nil {
		change["expires_at"] = wireTime(*in.ExpiresAt)
	}
	return audit.OutboxEvent{AggregateType: "resource_grant", AggregateID: provisioning.WireID("resource_grant", id.String()),
		EventType: "resource_grant.created", Change: change}
}

func auditRow(eventType, reason string, orgID, principalID, id uuid.UUID, a Actor, fields []string, extra map[string]any) audit.AuditEvent {
	org, subject := orgID.String(), principalID.String()
	ev := audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, OrganizationID: &org, SubjectPrincipalID: &subject,
		RequestID: a.RequestID, Attributes: map[string]any{"target_kind": "resource_grant", "target_id": id.String(),
			"reason_code": reason, "resource_kind": KindRepository, "changed_fields": fields}}
	for k, v := range extra {
		ev.Attributes[k] = v
	}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		ev.ActorPrincipalID = &actor
	}
	a.Caller.Annotate(ev.Attributes)
	return ev
}

// Create makes a new grant. The principal must hold an active membership of
// the organization. A live grant with the same role and key is
// ErrAlreadyGranted; a revoked one is history, so a re-grant is a new row.
func (s *Store) Create(ctx context.Context, in CreateInput) (*Grant, error) {
	if !s.ownsUsers {
		return nil, s.refuse(ctx, "create", ErrNotOwned)
	}
	in, err := checkCreate(in)
	if err != nil {
		return nil, s.refuse(ctx, "create", err)
	}
	id := uuid.New()
	for range planAttempts {
		err = s.run(ctx, "create", audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				st, err := s.lockPair(ctx, tx, in.Authorize, in.OrgID, in.PrincipalID, in.Actor.PrincipalID)
				if err != nil {
					return err
				}
				if !st.Active() {
					return ErrNotMember
				}
				var now time.Time
				if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
					return err
				}
				if in.ExpiresAt != nil && !in.ExpiresAt.After(now) {
					return ErrExpiryPast
				}
				var live int
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+s.q+`.resource_grants
WHERE principal_id = $1 AND organization_id = $2 AND role_key = $3 AND resource_kind = $4 AND resource_id = $5 AND revoked_at IS NULL`,
					in.PrincipalID, in.OrgID, in.Role, KindRepository, in.Key).Scan(&live); err != nil {
					return err
				}
				if live > 0 {
					return ErrAlreadyGranted
				}
				var expires, by any
				if in.ExpiresAt != nil {
					expires = *in.ExpiresAt
				}
				if in.Actor.PrincipalID != nil {
					by = *in.Actor.PrincipalID
				}
				_, err = tx.Exec(ctx, `INSERT INTO `+s.q+`.resource_grants
(id, principal_id, organization_id, role_key, resource_kind, resource_id, granted_by, expires_at, origin, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`, id, in.PrincipalID, in.OrgID, in.Role, KindRepository, in.Key, by, expires, string(in.Origin), now)
				return err
			},
			Audit: auditRow("auth.resource_grant.created", "grant_created", in.OrgID, in.PrincipalID, id, in.Actor, []string{"role_key", "status"},
				map[string]any{"new_status": "active"}),
			Events:   []audit.OutboxEvent{s.createEvent(id, in)},
			Refusals: []error{ErrNotMember, ErrAlreadyGranted, ErrExpiryPast},
		})
		if !errors.Is(err, errStateMoved) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, in.OrgID, id)
}

type row struct {
	principal uuid.UUID
	role      string
	key       string
	expires   *time.Time
	revoked   *time.Time
}

// lookup reads the grant lock-free (the plan read) or, with tx, locked.
func (s *Store) lookup(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, orgID, id uuid.UUID, lock bool) (row, bool, error) {
	sql := `SELECT principal_id, role_key, resource_id, expires_at, revoked_at FROM ` + s.q + `.resource_grants WHERE id = $1 AND organization_id = $2`
	if lock {
		sql += ` FOR UPDATE`
	}
	var r row
	err := q.QueryRow(ctx, sql, id, orgID).Scan(&r.principal, &r.role, &r.key, &r.expires, &r.revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return row{}, false, nil
	}
	return r, err == nil, err
}

func (s *Store) checkRevision(ctx context.Context, tx audit.TxOps, id uuid.UUID, expected *int64) error {
	if expected == nil {
		return nil
	}
	var rev int64
	err := tx.QueryRow(ctx, `SELECT revision FROM `+s.q+`.aggregate_revisions WHERE aggregate_type = 'resource_grant' AND aggregate_id = $1`,
		provisioning.WireID("resource_grant", id.String())).Scan(&rev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if rev != *expected {
		return ErrRevisionConflict
	}
	return nil
}

type poolRow struct{ pool *pgxpool.Pool }

func (p poolRow) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

// planPrincipal reads the grant's principal lock-free. A grant the plan read
// does not find plans against no principal: the locked read refuses after
// Authorize, so a refused actor learns nothing about which grants exist.
func (s *Store) planPrincipal(ctx context.Context, orgID, id uuid.UUID) (uuid.UUID, error) {
	r, found, err := s.lookup(ctx, poolRow{s.pool}, orgID, id, false)
	if err != nil {
		return uuid.Nil, err
	}
	if !found {
		return uuid.Nil, nil
	}
	return r.principal, nil
}

// Update changes the role or the expiry of a live grant.
func (s *Store) Update(ctx context.Context, in UpdateInput) (*Grant, error) {
	if !s.ownsUsers {
		return nil, s.refuse(ctx, "update", ErrNotOwned)
	}
	if err := checkUpdate(in); err != nil {
		return nil, s.refuse(ctx, "update", err)
	}
	var err error
	for range planAttempts {
		var principal uuid.UUID
		if principal, err = s.planPrincipal(ctx, in.OrgID, in.GrantID); err != nil {
			return nil, s.refuse(ctx, "update", fmt.Errorf("%w: update: %w", ErrStore, err))
		}
		planned := principal
		// The audit row and event are never written when the locked read
		// refuses; they only have to be well formed before the transaction.
		fields := []string{"role_key"}
		change := map[string]any{"status": "active"}
		if in.Role != nil {
			if set := s.actionSet(*in.Role); set != nil {
				change["action_set"] = set
			}
		}
		if in.ExpiresAt != nil {
			change["expires_at"] = wireTime(*in.ExpiresAt)
		}
		if in.ExpiresAt != nil || in.ClearExpiry {
			fields = append(fields, "expires_at")
		}
		err = s.run(ctx, "update", audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				if _, err := s.lockPair(ctx, tx, in.Authorize, in.OrgID, planned, in.Actor.PrincipalID); err != nil {
					if planned == uuid.Nil && errors.Is(err, ErrPrincipalNotFound) {
						return ErrNotFound
					}
					return err
				}
				cur, found, err := s.lookup(ctx, txRow{tx}, in.OrgID, in.GrantID, true)
				if err != nil {
					return err
				}
				if !found || cur.principal != planned {
					if planned == uuid.Nil && !found {
						return ErrNotFound
					}
					return errStateMoved
				}
				if cur.revoked != nil {
					return ErrRevoked
				}
				if err := s.checkRevision(ctx, tx, in.GrantID, in.ExpectedRevision); err != nil {
					return err
				}
				var now time.Time
				if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
					return err
				}
				if in.ExpiresAt != nil && !in.ExpiresAt.After(now) {
					return ErrExpiryPast
				}
				role, expires := cur.role, cur.expires
				if in.Role != nil {
					role = *in.Role
				}
				switch {
				case in.ClearExpiry:
					expires = nil
				case in.ExpiresAt != nil:
					expires = in.ExpiresAt
				}
				if role == cur.role && sameTime(expires, cur.expires) {
					return nil
				}
				var exp any
				if expires != nil {
					exp = *expires
				}
				_, err = tx.Exec(ctx, `UPDATE `+s.q+`.resource_grants SET role_key = $2, expires_at = $3, updated_at = $4 WHERE id = $1`,
					in.GrantID, role, exp, now)
				return err
			},
			Audit: auditRow("auth.resource_grant.updated", "grant_updated", in.OrgID, principalOrPlaceholder(planned), in.GrantID, in.Actor, fields, nil),
			Events: []audit.OutboxEvent{{AggregateType: "resource_grant", AggregateID: provisioning.WireID("resource_grant", in.GrantID.String()),
				EventType: "resource_grant.updated", Change: change}},
			Refusals: []error{ErrNotFound, ErrRevoked, ErrExpiryPast, errStateMoved},
		})
		if !errors.Is(err, errStateMoved) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, in.OrgID, in.GrantID)
}

func principalOrPlaceholder(id uuid.UUID) uuid.UUID {
	if id == uuid.Nil {
		return uuid.New()
	}
	return id
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

type txRow struct{ tx audit.TxOps }

func (t txRow) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

// Revoke ends a grant. Revoking a revoked grant changes nothing.
func (s *Store) Revoke(ctx context.Context, in RevokeInput) (*Grant, error) {
	if !s.ownsUsers {
		return nil, s.refuse(ctx, "revoke", ErrNotOwned)
	}
	var err error
	for range planAttempts {
		var principal uuid.UUID
		if principal, err = s.planPrincipal(ctx, in.OrgID, in.GrantID); err != nil {
			return nil, s.refuse(ctx, "revoke", fmt.Errorf("%w: revoke: %w", ErrStore, err))
		}
		planned := principal
		who := grantend.Actor{PrincipalID: in.Actor.PrincipalID, RequestID: in.Actor.RequestID, Caller: in.Actor.Caller}
		err = s.run(ctx, "revoke", audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				if _, err := s.lockPair(ctx, tx, in.Authorize, in.OrgID, planned, in.Actor.PrincipalID); err != nil {
					if planned == uuid.Nil && errors.Is(err, ErrPrincipalNotFound) {
						return ErrNotFound
					}
					return err
				}
				cur, found, err := s.lookup(ctx, txRow{tx}, in.OrgID, in.GrantID, true)
				if err != nil {
					return err
				}
				if !found || cur.principal != planned {
					if planned == uuid.Nil && !found {
						return ErrNotFound
					}
					return errStateMoved
				}
				if err := s.checkRevision(ctx, tx, in.GrantID, in.ExpectedRevision); err != nil {
					return err
				}
				if cur.revoked != nil {
					return nil
				}
				p, err := grantend.PlanGrant(ctx, grantend.TxReader(tx), s.q, in.OrgID, in.GrantID)
				if err != nil {
					return err
				}
				if err := p.Apply(ctx, tx, in.Actor.PrincipalID); err != nil {
					if errors.Is(err, grantend.ErrStateMoved) {
						return errStateMoved
					}
					return err
				}
				return nil
			},
			Audit:    grantend.AuditFor(in.OrgID, principalOrPlaceholder(planned), in.GrantID, who),
			Events:   []audit.OutboxEvent{grantend.EventFor(in.GrantID)},
			Refusals: []error{ErrNotFound, errStateMoved},
		})
		if !errors.Is(err, errStateMoved) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, in.OrgID, in.GrantID)
}

const selectGrant = `SELECT g.id, g.organization_id, g.principal_id, g.role_key, g.resource_id, g.expires_at, g.origin, g.granted_by,
g.revoked_at, g.revoked_by, g.created_at, coalesce(r.revision, 0)`

func (s *Store) from() string {
	return ` FROM ` + s.q + `.resource_grants g LEFT JOIN ` + s.q + `.aggregate_revisions r
ON r.aggregate_type = 'resource_grant' AND r.aggregate_id = 'grt_' || g.id::text`
}

func scanGrant(row pgx.Row) (*Grant, error) {
	var g Grant
	var origin string
	if err := row.Scan(&g.ID, &g.OrgID, &g.PrincipalID, &g.Role, &g.Key, &g.ExpiresAt, &origin, &g.GrantedBy, &g.RevokedAt, &g.RevokedBy,
		&g.CreatedAt, &g.Revision); err != nil {
		return nil, err
	}
	g.Origin = Origin(origin)
	return &g, nil
}

// Get reads one grant of an organization.
func (s *Store) Get(ctx context.Context, orgID, id uuid.UUID) (*Grant, error) {
	g, err := scanGrant(s.pool.QueryRow(ctx, selectGrant+s.from()+` WHERE g.id = $1 AND g.organization_id = $2`, id, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w: get: %w", ErrStore, err)
	}
	return g, nil
}

// ListFilter narrows ListForOrg.
type ListFilter struct {
	PrincipalID    *uuid.UUID
	IncludeRevoked bool
}

// ListForOrg lists the grants of an organization by id.
func (s *Store) ListForOrg(ctx context.Context, orgID uuid.UUID, f ListFilter) ([]Grant, error) {
	sql := selectGrant + s.from() + ` WHERE g.organization_id = $1 AND ($2::uuid IS NULL OR g.principal_id = $2)
AND ($3 OR g.revoked_at IS NULL) ORDER BY g.id`
	var principal any
	if f.PrincipalID != nil {
		principal = *f.PrincipalID
	}
	rows, err := s.pool.Query(ctx, sql, orgID, principal, f.IncludeRevoked)
	if err != nil {
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	defer rows.Close()
	out := []Grant{}
	for rows.Next() {
		g, err := scanGrant(rows)
		if err != nil {
			return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
		}
		out = append(out, *g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: list: %w", ErrStore, err)
	}
	return out, nil
}
