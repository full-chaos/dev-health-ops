// Package decisionstate reads the policy decision point's state: one READ ONLY
// REPEATABLE READ transaction per authorize call, over the auth schema and
// go-api's public users and memberships, so every read of the call sees one
// snapshot.
package decisionstate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrStore wraps every failed read. The error text never carries a value.
var ErrStore = errors.New("decisionstate: state unavailable")

// principalKindUser is the only principal kind with a decision rule today:
// public.users holds user principals only. Every other kind denies until a
// leaf names its rule.
const principalKindUser = "user"

// Config builds a Reader.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// Users reads public.users (is_active, and is_superuser or the platform
	// role rule while platform roles are owned).
	Users authapi.PGUsers
}

// Reader is an authapi.DecisionState over Postgres.
type Reader struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
	users  authapi.PGUsers
	roles  *platformrole.Store
}

// New validates cfg.
func New(cfg Config) (*Reader, error) {
	if cfg.Pool == nil {
		return nil, errors.New("decisionstate: a pool is required")
	}
	if cfg.Schema == (authschema.ValidatedIdentifier{}) {
		return nil, errors.New("decisionstate: a validated auth schema is required")
	}
	roles, err := platformrole.New(platformrole.Config{Pool: cfg.Pool, Schema: cfg.Schema})
	if err != nil {
		return nil, err
	}
	return &Reader{pool: cfg.Pool, schema: cfg.Schema, users: cfg.Users, roles: roles}, nil
}

func storeErr(what string, err error) error { return fmt.Errorf("%w: %s: %w", ErrStore, what, err) }

// Read runs fn in one READ ONLY REPEATABLE READ transaction and rolls it back.
func (r *Reader) Read(ctx context.Context, fn func(authapi.DecisionTx) error) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return storeErr("begin", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return storeErr("clock", err)
	}
	return fn(&txReader{tx: tx, r: r, now: now.UnixNano()})
}

type txReader struct {
	tx  pgx.Tx
	r   *Reader
	now int64
}

func (t *txReader) table(name string) string { return authschema.Quote(t.r.schema) + "." + name }

// Session reads the session row: live when not revoked and not expired at the
// transaction's clock, the delegation columns, and whether the delegating
// session is live by the same rule.
func (t *txReader) Session(ctx context.Context, sessionID uuid.UUID) (authapi.DecisionSession, error) {
	var (
		s            authapi.DecisionSession
		actor, orgID *string
	)
	err := t.tx.QueryRow(ctx, `SELECT s.principal_id, s.revoked_at IS NULL AND s.expires_at > now(),
s.actor_principal_id::text, s.delegated_from_session_id IS NOT NULL, s.organization_id::text, s.amr, s.delegation_permitted_actions,
COALESCE((SELECT b.revoked_at IS NULL AND b.expires_at > now() FROM `+t.table("sessions")+` b WHERE b.id = s.delegated_from_session_id), false)
FROM `+t.table("sessions")+` s WHERE s.id = $1`,
		sessionID).Scan(&s.PrincipalID, &s.Live, &actor, &s.HasDelegatedFrom, &orgID, &s.AMR, &s.PermittedActions, &s.BaseLive)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authapi.DecisionSession{}, nil
	case err != nil:
		return authapi.DecisionSession{}, storeErr("session", err)
	}
	if actor != nil {
		id, err := uuid.Parse(*actor)
		if err != nil {
			return authapi.DecisionSession{}, storeErr("session", err)
		}
		s.HasActor, s.ActorPrincipalID = true, id
	}
	if orgID != nil {
		id, err := uuid.Parse(*orgID)
		if err != nil {
			return authapi.DecisionSession{}, storeErr("session", err)
		}
		s.OrganizationID = &id
	}
	s.Found = true
	return s, nil
}

// Actor reads the real actor of a delegated session now, in the decision
// transaction: an active user principal whose public.users row is active, and
// the platform roles it holds. A principal that is no user is the zero state.
func (t *txReader) Actor(ctx context.Context, actor uuid.UUID) (authapi.ActorState, error) {
	kind, enabled, found, err := t.principalRow(ctx, actor)
	if err != nil || !found || kind != principalKindUser {
		return authapi.ActorState{}, err
	}
	user, found, err := t.r.users.StateOn(ctx, t.tx, actor)
	if err != nil {
		return authapi.ActorState{}, storeErr("user", err)
	}
	if !found {
		return authapi.ActorState{}, nil
	}
	held, err := t.r.roles.ResolveOn(ctx, t.tx, actor)
	if err != nil {
		return authapi.ActorState{}, storeErr("platform role", err)
	}
	return authapi.ActorState{
		Active:         enabled && user.IsActive,
		CanImpersonate: held.Allows(session.ImpersonateAction),
		Owner:          held.IsOwner(),
	}, nil
}

// Snapshot reads the principal's state for orgID ("" for a platform
// decision). An org id that is not a canonical uuid names no organization: its
// status is unknown and nothing is read for it.
func (t *txReader) Snapshot(ctx context.Context, principal uuid.UUID, orgID string) (authorization.GrantSnapshot, error) {
	s := &snapshot{org: orgID, now: t.now}
	if err := t.principal(ctx, principal, s); err != nil {
		return nil, err
	}
	org, err := uuid.Parse(orgID)
	if orgID == "" || err != nil || org.String() != orgID {
		return s, nil
	}
	if err := t.organization(ctx, principal, org, s); err != nil {
		return nil, err
	}
	return s, nil
}

// principalRow reads the principal's kind and whether it is not disabled.
func (t *txReader) principalRow(ctx context.Context, principal uuid.UUID) (kind string, enabled, found bool, err error) {
	err = t.tx.QueryRow(ctx, `SELECT kind, disabled_at IS NULL FROM `+t.table("principals")+` WHERE id = $1`, principal).Scan(&kind, &enabled)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", false, false, nil
	case err != nil:
		return "", false, false, storeErr("principal", err)
	}
	return kind, enabled, true, nil
}

// principal reads the principal's kind and disable mark, then, for a user,
// go-api's users row. A user is active only when both say so.
func (t *txReader) principal(ctx context.Context, principal uuid.UUID, s *snapshot) error {
	kind, enabled, found, err := t.principalRow(ctx, principal)
	if err != nil || !found || kind != principalKindUser {
		return err
	}
	user, found, err := t.r.users.StateOn(ctx, t.tx, principal)
	if err != nil {
		return storeErr("user", err)
	}
	if !found {
		return nil
	}
	if enabled && user.IsActive {
		s.principal = authorization.StatusActive
	} else {
		s.principal = authorization.StatusSuspended
	}
	if user.IsSuperuser {
		s.platformRole = rule.PlatformAdmin
	}
	return nil
}

// organization reads the organization's status, the effective membership and
// every grant row of the pair, in any state.
func (t *txReader) organization(ctx context.Context, principal, org uuid.UUID, s *snapshot) error {
	var active bool
	err := t.tx.QueryRow(ctx, `SELECT disabled_at IS NULL FROM `+t.table("organizations")+` WHERE id = $1`, org).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		s.orgStatus = authorization.StatusUnknown
	case err != nil:
		return storeErr("organization", err)
	case active:
		s.orgStatus = authorization.StatusActive
	default:
		s.orgStatus = authorization.StatusSuspended
	}
	m, err := membership.EffectiveOn(ctx, t.tx, t.r.schema, org, principal)
	if err != nil {
		return storeErr("membership", err)
	}
	if m.Found {
		s.membership = &authorization.Membership{OrgID: s.org, Role: m.Role, Status: authorization.ParseStatus(string(m.Status))}
	}
	rows, err := t.tx.Query(ctx, `SELECT role_key, resource_kind, resource_id, expires_at, revoked_at IS NOT NULL
FROM `+t.table("resource_grants")+` WHERE principal_id = $1 AND organization_id = $2`, principal, org)
	if err != nil {
		return storeErr("grants", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			g       authorization.Grant
			expires *time.Time
		)
		if err := rows.Scan(&g.Role, &g.ResourceKind, &g.ResourceKey, &expires, &g.Revoked); err != nil {
			return storeErr("grants", err)
		}
		g.OrgID = s.org
		if expires != nil {
			g.ExpiresAtUnixNano = expires.UnixNano()
		}
		s.grants = append(s.grants, g)
	}
	if err := rows.Err(); err != nil {
		return storeErr("grants", err)
	}
	return nil
}

// snapshot is one (principal, organization) state. It holds no connection:
// every value was read before the decision core sees it.
type snapshot struct {
	principal    authorization.Status
	platformRole string
	org          string
	orgStatus    authorization.Status
	membership   *authorization.Membership
	grants       []authorization.Grant
	now          int64
}

func (s *snapshot) PrincipalStatus() authorization.Status { return s.principal }

func (s *snapshot) PlatformRole() string { return s.platformRole }

func (s *snapshot) OrganizationStatus(orgID string) authorization.Status {
	if orgID == "" || orgID != s.org {
		return authorization.StatusUnknown
	}
	return s.orgStatus
}

func (s *snapshot) Membership(orgID string) (authorization.Membership, bool) {
	if orgID == "" || orgID != s.org || s.membership == nil {
		return authorization.Membership{}, false
	}
	return *s.membership, true
}

func (s *snapshot) Grants(orgID string) ([]authorization.Grant, error) {
	if orgID == "" || orgID != s.org {
		return nil, nil
	}
	return s.grants, nil
}

func (s *snapshot) Workload() (authorization.WorkloadLimits, bool) {
	return authorization.WorkloadLimits{}, false
}

func (s *snapshot) ServiceAccount(string) (authorization.ServiceAccount, bool) {
	return authorization.ServiceAccount{}, false
}

func (s *snapshot) NowUnixNano() int64 { return s.now }

var _ authapi.DecisionState = (*Reader)(nil)
