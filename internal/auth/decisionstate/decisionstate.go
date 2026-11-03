// Package decisionstate reads the policy decision point's state: one READ ONLY
// REPEATABLE READ transaction per authorize call, over the auth schema and
// go-api's public users and memberships, so every read of the call sees one
// snapshot.
package decisionstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
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
	// RefreshTTL is the refresh lifetime the session store reads liveness with. It is required: the
	// caller passes the one value the store gets (session.DefaultRefreshTTL), never a second default.
	RefreshTTL time.Duration
}

// Reader is an authapi.DecisionState over Postgres.
type Reader struct {
	pool    *pgxpool.Pool
	schema  authschema.ValidatedIdentifier
	users   authapi.PGUsers
	roles   *platformrole.Store
	collect bool
	rule    sessionrule.Rule
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
	rule, err := sessionrule.New(authschema.Quote(cfg.Schema), cfg.RefreshTTL)
	if err != nil {
		return nil, fmt.Errorf("decisionstate: %w", err)
	}
	return &Reader{pool: cfg.Pool, schema: cfg.Schema, users: cfg.Users, roles: roles, collect: cfg.CollectDependencies, rule: rule}, nil
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

// Session reads the session row: live by the session rule (sessionrule: not revoked, not stamp-voided,
// its web session not ended, not expired) at the transaction's clock, the delegation columns, and whether
// the delegating session is live by the same rule.
func (t *txReader) Session(ctx context.Context, sessionID uuid.UUID) (authapi.DecisionSession, error) {
	var (
		s            authapi.DecisionSession
		actor, orgID *string
	)
	err := t.tx.QueryRow(ctx, sessionQuery(t.r.rule, t.table("sessions")), sessionID).Scan(
		&s.PrincipalID, &s.Live, &actor, &s.HasDelegatedFrom, &orgID, &s.AMR, &s.PermittedActions, &s.BaseLive)
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

// sessionQuery reads one session row ($1) with Live and BaseLive from one rule. now() is the
// transaction's clock, so every read of a call sees one instant.
func sessionQuery(rule sessionrule.Rule, sessions string) string {
	return `SELECT s.principal_id, ` + rule.Live("s", "now()") + `,
s.actor_principal_id::text, s.delegated_from_session_id IS NOT NULL, s.organization_id::text, s.amr, s.delegation_permitted_actions,
COALESCE((SELECT ` + rule.Live("b", "now()") + ` FROM ` + sessions + ` b WHERE b.id = s.delegated_from_session_id), false)
FROM ` + sessions + ` s WHERE s.id = $1`
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

// StateVersion is a digest of (table, id, xmin) of every row a decision of the pair depends on, in the transaction's
// snapshot: the principal, the organization, the pair's membership row, every grant row of the pair, and the principal's
// platform role assignments while auth owns them. Any committed insert, update or delete of one of them is a new
// digest, with or without an outbox event; VACUUM FREEZE keeps xmin.
func (t *txReader) StateVersion(ctx context.Context, principal uuid.UUID, orgID string) (string, error) {
	org, err := uuid.Parse(orgID)
	read := orgID != "" && err == nil && org.String() == orgID
	args := []any{principal}
	if read {
		args = append(args, org)
	}
	rows, err := t.tx.Query(ctx, stateQuery(authschema.Quote(t.r.schema), read, t.r.users.PlatformRolesOwned), args...)
	if err != nil {
		return "", storeErr("state version", err)
	}
	defer rows.Close()
	var found []stateRow
	for rows.Next() {
		var r stateRow
		if err := rows.Scan(&r.table, &r.id, &r.xmin); err != nil {
			return "", storeErr("state version", err)
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return "", storeErr("state version", err)
	}
	return stateDigest(found), nil
}

// stateRow is one row version: its table, id and xmin.
type stateRow struct{ table, id, xmin string }

// stateQuery reads the row versions of the pair: $1 is the principal, $2 the organization when org is set.
func stateQuery(schema string, org, owned bool) string {
	parts := []string{`SELECT 'principals', id::text, xmin::text FROM ` + schema + `.principals WHERE id = $1`}
	if org {
		parts = append(parts,
			`SELECT 'organizations', id::text, xmin::text FROM `+schema+`.organizations WHERE id = $2`,
			`SELECT 'memberships', id::text, xmin::text FROM `+schema+`.memberships WHERE organization_id = $2 AND principal_id = $1`,
			`SELECT 'resource_grants', id::text, xmin::text FROM `+schema+`.resource_grants WHERE principal_id = $1 AND organization_id = $2`)
	}
	if owned {
		parts = append(parts, `SELECT 'platform_role_assignments', id::text, xmin::text FROM `+schema+`.platform_role_assignments WHERE principal_id = $1`)
	}
	return strings.Join(parts, "\nUNION ALL ")
}

// stateDigest is the sha256 of the sorted rows, so the order the database returns them in does not count.
func stateDigest(rows []stateRow) string {
	lines := make([]string, len(rows))
	for i, r := range rows {
		lines[i] = r.table + "\x00" + r.id + "\x00" + r.xmin
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
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
	if err != nil || !found {
		return err
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
