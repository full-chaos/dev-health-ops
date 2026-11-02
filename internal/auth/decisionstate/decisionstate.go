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
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
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
	// CollectDependencies makes every snapshot say what it read (the aggregate
	// ids and their revisions, in the same transaction) for the decision cache.
	CollectDependencies bool
}

// Reader is an authapi.DecisionState over Postgres.
type Reader struct {
	pool    *pgxpool.Pool
	schema  authschema.ValidatedIdentifier
	users   authapi.PGUsers
	collect bool
}

// New validates cfg.
func New(cfg Config) (*Reader, error) {
	if cfg.Pool == nil {
		return nil, errors.New("decisionstate: a pool is required")
	}
	if cfg.Schema == (authschema.ValidatedIdentifier{}) {
		return nil, errors.New("decisionstate: a validated auth schema is required")
	}
	return &Reader{pool: cfg.Pool, schema: cfg.Schema, users: cfg.Users, collect: cfg.CollectDependencies}, nil
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
// transaction's clock, and whether it names an actor or a delegating session.
func (t *txReader) Session(ctx context.Context, sessionID uuid.UUID) (authapi.DecisionSession, error) {
	var s authapi.DecisionSession
	err := t.tx.QueryRow(ctx, `SELECT principal_id, revoked_at IS NULL AND expires_at > now(),
actor_principal_id IS NOT NULL, delegated_from_session_id IS NOT NULL FROM `+t.table("sessions")+` WHERE id = $1`,
		sessionID).Scan(&s.PrincipalID, &s.Live, &s.HasActor, &s.HasDelegatedFrom)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return authapi.DecisionSession{}, nil
	case err != nil:
		return authapi.DecisionSession{}, storeErr("session", err)
	}
	s.Found = true
	return s, nil
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
	read := orgID != "" && err == nil && org.String() == orgID
	if read {
		if err := t.organization(ctx, principal, org, s); err != nil {
			return nil, err
		}
	}
	if t.r.collect {
		s.sets = setTypes(read, t.r.users.PlatformRolesOwned)
		if s.deps, err = t.revisions(ctx, s.deps); err != nil {
			return nil, err
		}
	}
	return snapshotOut(s, t.r.collect), nil
}

// Unfed reads what no feed event covers (go-api writes public.users and public.memberships without an outbox event), for
// the decision cache key. The users row is read through PGUsers.StateOn, as the snapshot reads it; its platform role
// counts only while auth does not own the platform roles: owned, the role comes from auth's assignments, which the feed
// covers. A public membership row with a NULL role is no row, as effective() reads it.
func (t *txReader) Unfed(ctx context.Context, principal uuid.UUID, orgID string) (authapi.UnfedState, error) {
	u := authapi.UnfedState{NowUnixNano: t.now}
	user, found, err := t.r.users.StateOn(ctx, t.tx, principal)
	if err != nil {
		return authapi.UnfedState{}, storeErr("unfed user", err)
	}
	u.UserFound, u.UserActive = found, found && user.IsActive
	if !t.r.users.PlatformRolesOwned {
		superuser := found && user.IsSuperuser
		u.Superuser = &superuser
	}
	org, err := uuid.Parse(orgID)
	if orgID == "" || err != nil || org.String() != orgID {
		return u, nil
	}
	var role *string
	err = t.tx.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`, principal, org).Scan(&role)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return authapi.UnfedState{}, storeErr("unfed membership", err)
	case role != nil:
		u.MembershipRoleFound, u.MembershipRole = true, *role
	}
	return u, nil
}

// revisions reads the revision of each dependency in the transaction; an aggregate with no row yet is revision 0.
func (t *txReader) revisions(ctx context.Context, deps []decisioncache.Dependency) ([]decisioncache.Dependency, error) {
	if len(deps) == 0 {
		return deps, nil
	}
	types, ids := make([]string, len(deps)), make([]string, len(deps))
	for i, d := range deps {
		types[i], ids[i] = d.AggregateType, d.AggregateID
	}
	rows, err := t.tx.Query(ctx, `SELECT aggregate_type, aggregate_id, revision FROM `+t.table("aggregate_revisions")+`
WHERE (aggregate_type, aggregate_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))`, types, ids)
	if err != nil {
		return nil, storeErr("revisions", err)
	}
	defer rows.Close()
	found := map[string]int64{}
	for rows.Next() {
		var (
			typ, id string
			rev     int64
		)
		if err := rows.Scan(&typ, &id, &rev); err != nil {
			return nil, storeErr("revisions", err)
		}
		found[typ+":"+id] = rev
	}
	if err := rows.Err(); err != nil {
		return nil, storeErr("revisions", err)
	}
	return withRevisions(deps, found), nil
}

// principalDependencies are the principal and user aggregates of a principal, under the writers' wire ids.
func principalDependencies(principal uuid.UUID) []decisioncache.Dependency {
	return []decisioncache.Dependency{
		{AggregateType: "principal", AggregateID: provisioning.WireID("principal", principal.String())},
		{AggregateType: "user", AggregateID: provisioning.WireID("user", principal.String())},
	}
}

// organizationDependencies are the organization, the auth membership row (when one exists) and each grant row read.
func organizationDependencies(org uuid.UUID, membershipID *uuid.UUID, grants []uuid.UUID) []decisioncache.Dependency {
	deps := []decisioncache.Dependency{{AggregateType: "organization", AggregateID: provisioning.WireID("organization", org.String())}}
	if membershipID != nil {
		deps = append(deps, decisioncache.Dependency{AggregateType: "membership", AggregateID: provisioning.WireID("membership", membershipID.String())})
	}
	for _, g := range grants {
		deps = append(deps, decisioncache.Dependency{AggregateType: "resource_grant", AggregateID: provisioning.WireID("resource_grant", g.String())})
	}
	return deps
}

// withRevisions sets each dependency's revision from found ("type:id"); a missing one is 0.
func withRevisions(deps []decisioncache.Dependency, found map[string]int64) []decisioncache.Dependency {
	out := make([]decisioncache.Dependency, len(deps))
	for i, d := range deps {
		d.Revision = found[d.AggregateType+":"+d.AggregateID]
		out[i] = d
	}
	return out
}

// setTypes are the aggregate types read as a set, where a row that did not exist is a dependency too: the platform role
// assignments while auth owns them, and the organization's memberships and grants.
func setTypes(org, owned bool) []string {
	var sets []string
	if owned {
		sets = append(sets, "platform_role_assignment")
	}
	if org {
		sets = append(sets, "membership", "resource_grant")
	}
	return sets
}

// collectedSnapshot is a snapshot that says what it read (authapi.DependencySnapshot).
type collectedSnapshot struct{ *snapshot }

func (c collectedSnapshot) Dependencies() []decisioncache.Dependency { return c.deps }
func (c collectedSnapshot) SetTypes() []string                       { return c.sets }

// snapshotOut gives the dependencies only when the reader collected them, so a decision whose reads were not recorded is
// never stored.
func snapshotOut(s *snapshot, collect bool) authorization.GrantSnapshot {
	if collect {
		return collectedSnapshot{s}
	}
	return s
}

// principal reads the principal's kind and disable mark, then, for a user,
// go-api's users row. A user is active only when both say so.
func (t *txReader) principal(ctx context.Context, principal uuid.UUID, s *snapshot) error {
	var (
		kind    string
		enabled bool
	)
	err := t.tx.QueryRow(ctx, `SELECT kind, disabled_at IS NULL FROM `+t.table("principals")+` WHERE id = $1`, principal).Scan(&kind, &enabled)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return storeErr("principal", err)
	}
	if t.r.collect {
		s.deps = append(s.deps, principalDependencies(principal)...)
	}
	if kind != principalKindUser {
		return nil
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
	var membershipID *uuid.UUID
	if t.r.collect {
		var id uuid.UUID
		err := t.tx.QueryRow(ctx, `SELECT id FROM `+t.table("memberships")+` WHERE organization_id = $1 AND principal_id = $2`, org, principal).Scan(&id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return storeErr("membership id", err)
		default:
			membershipID = &id
		}
	}
	if m.Found {
		s.membership = &authorization.Membership{OrgID: s.org, Role: m.Role, Status: authorization.ParseStatus(string(m.Status))}
	}
	rows, err := t.tx.Query(ctx, `SELECT id, role_key, resource_kind, resource_id, expires_at, revoked_at IS NOT NULL
FROM `+t.table("resource_grants")+` WHERE principal_id = $1 AND organization_id = $2`, principal, org)
	if err != nil {
		return storeErr("grants", err)
	}
	defer rows.Close()
	var grantIDs []uuid.UUID
	for rows.Next() {
		var (
			id      uuid.UUID
			g       authorization.Grant
			expires *time.Time
		)
		if err := rows.Scan(&id, &g.Role, &g.ResourceKind, &g.ResourceKey, &expires, &g.Revoked); err != nil {
			return storeErr("grants", err)
		}
		grantIDs = append(grantIDs, id)
		g.OrgID = s.org
		if expires != nil {
			g.ExpiresAtUnixNano = expires.UnixNano()
		}
		s.grants = append(s.grants, g)
	}
	if err := rows.Err(); err != nil {
		return storeErr("grants", err)
	}
	if t.r.collect {
		s.deps = append(s.deps, organizationDependencies(org, membershipID, grantIDs)...)
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
	// deps and sets are what the snapshot read, for the decision cache (Reader.collect).
	deps []decisioncache.Dependency
	sets []string
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
