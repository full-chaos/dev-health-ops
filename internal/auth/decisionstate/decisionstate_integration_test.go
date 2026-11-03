//go:build integration

package decisionstate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationinput"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	identity *identity.Store
	reader   *Reader
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	closers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

func newEnv(t *testing.T) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("decisionstate_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "decisionstate-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	reader, err := New(Config{Pool: runtime, Schema: schema, Users: authapi.PGUsers{Pool: runtime, AuthSchema: schema}})
	if err != nil {
		return nil, err
	}
	if _, err := admin.Exec(ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('viewer', 'viewer', 'organization') ON CONFLICT DO NOTHING`); err != nil {
		return nil, fmt.Errorf("seed role: %w", err)
	}
	return &env{admin: admin, runtime: runtime, schema: schema, identity: id, reader: reader}, nil
}

func (e *env) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// armed writes a guarded security table the way a test seed may: in an admin
// transaction with the triggers off (session_replication_role replica), as the
// organization deprovision fixtures do, so this file names no guard setting.
func (e *env) armed(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit %s: %v", sql, err)
	}
}

// user seeds go-api's users row and projects its principal.
func (e *env) user(t *testing.T, ctx context.Context, active, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', $3, true, $4, 0, now(), now())`, id, "ds-"+id.String()[:8]+"@example.com", active, superuser)
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) org(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'decisionstate-test')`, id, "org-"+id.String()[:8])
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

// publicUser seeds go-api's users row only: there is no auth copy.
func (e *env) publicUser(t *testing.T, ctx context.Context, active, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', $3, true, $4, 0, now(), now())`, id, "ds-"+id.String()[:8]+"@example.com", active, superuser)
	return id
}

// publicOrg seeds go-api's organizations row only: there is no auth copy.
func (e *env) publicOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'decisionstate-test')`, id, "org-"+id.String()[:8])
	return id
}

func (e *env) snapshot(t *testing.T, ctx context.Context, principal uuid.UUID, org string) authorization.GrantSnapshot {
	t.Helper()
	var snap authorization.GrantSnapshot
	if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		snap, err = tx.Snapshot(ctx, principal, org)
		return err
	}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	return snap
}

func TestSnapshotReadsAnActiveMemberFromBothSchemas(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'admin', now())`, uuid.New(), user, org)
	snap := e.snapshot(t, ctx, user, org.String())
	if snap.PrincipalStatus() != authorization.StatusActive || snap.PlatformRole() != "" || snap.OrganizationStatus(org.String()) != authorization.StatusActive {
		t.Fatalf("principal %v platform %q org %v", snap.PrincipalStatus(), snap.PlatformRole(), snap.OrganizationStatus(org.String()))
	}
	m, ok := snap.Membership(org.String())
	if !ok || m.Role != "admin" || m.Status != authorization.StatusActive || m.OrgID != org.String() {
		t.Fatalf("membership %+v %v, want active admin", m, ok)
	}
	if snap.NowUnixNano() <= 0 {
		t.Fatal("no clock")
	}
	if _, ok := snap.Membership(uuid.NewString()); ok {
		t.Fatal("a membership of another organization was reported")
	}
}

func TestSnapshotAuthMembershipWithoutAPublicRowIsNoMembership(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.org(t, ctx)
	for _, status := range []string{"active", "suspended", "invited"} {
		other := e.user(t, ctx, true, false)
		e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, $3, 'admin', now())`, org, other, status)
		if _, ok := e.snapshot(t, ctx, other, org.String()).Membership(org.String()); ok {
			t.Fatalf("an auth membership %s with no public row read as a membership", status)
		}
		d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: "settings.write", ResourceKind: "organization", OrganizationID: org.String()},
			e.snapshot(t, ctx, other, org.String()))
		if d.Allowed || d.Reason != authorization.ReasonNoMembership {
			t.Fatalf("auth membership %s, no public row: %+v, want no_membership", status, d)
		}
	}
}

func TestSnapshotPrincipalIsThePublicUsersRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	inactive := e.user(t, ctx, false, false)
	if s := e.snapshot(t, ctx, inactive, "").PrincipalStatus(); s != authorization.StatusSuspended {
		t.Fatalf("users.is_active false: %v, want suspended", s)
	}
	disabledCopy := e.user(t, ctx, true, false)
	e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, disabledCopy)
	if s := e.snapshot(t, ctx, disabledCopy, "").PrincipalStatus(); s != authorization.StatusActive {
		t.Fatalf("a disabled auth copy of an active public user: %v, want active (public wins)", s)
	}
	noCopy := e.publicUser(t, ctx, true, false)
	if s := e.snapshot(t, ctx, noCopy, "").PrincipalStatus(); s != authorization.StatusActive {
		t.Fatalf("an active public user with no auth copy: %v, want active", s)
	}
	for _, kind := range []string{"user", "service_account", "workload"} {
		id := uuid.New()
		e.armed(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, $2)`, id, kind)
		if s := e.snapshot(t, ctx, id, "").PrincipalStatus(); s != authorization.StatusUnknown {
			t.Fatalf("auth principal of kind %s with no public.users row: %v, want unknown", kind, s)
		}
	}
	if s := e.snapshot(t, ctx, uuid.New(), "").PrincipalStatus(); s != authorization.StatusUnknown {
		t.Fatalf("an unknown principal: %v, want unknown", s)
	}
	admin := e.user(t, ctx, true, true)
	if r := e.snapshot(t, ctx, admin, "").PlatformRole(); r != "platform_admin" {
		t.Fatalf("superuser platform role %q", r)
	}
}

func TestSnapshotReadsEveryGrantRowWithItsState(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, owner, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/live')`, user, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, expires_at) VALUES ($1, $2, 'viewer', 'repository', 'acme/old', now() - interval '1 day')`, user, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, revoked_at, revoked_by) VALUES ($1, $2, 'viewer', 'repository', 'acme/gone', now(), $3)`, user, org, owner)
	other := e.org(t, ctx)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id) VALUES ($1, $2, 'viewer', 'repository', 'acme/elsewhere')`, user, other)
	grants, err := e.snapshot(t, ctx, user, org.String()).Grants(org.String())
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]authorization.Grant{}
	for _, g := range grants {
		byKey[g.ResourceKey] = g
	}
	if len(grants) != 3 {
		t.Fatalf("grants %+v, want the three rows of this organization", grants)
	}
	if g := byKey["acme/live"]; g.Revoked || g.ExpiresAtUnixNano != 0 || g.OrgID != org.String() || g.Role != "viewer" || g.ResourceKind != "repository" {
		t.Fatalf("live grant %+v", g)
	}
	if g := byKey["acme/old"]; g.ExpiresAtUnixNano == 0 || g.ExpiresAtUnixNano > time.Now().UnixNano() {
		t.Fatalf("expired grant %+v", g)
	}
	if !byKey["acme/gone"].Revoked {
		t.Fatalf("revoked grant %+v", byKey["acme/gone"])
	}
}

func TestSnapshotOrganizationStatus(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, org)
	if s := e.snapshot(t, ctx, user, org.String()).OrganizationStatus(org.String()); s != authorization.StatusSuspended {
		t.Fatalf("inactive public organization: %v", s)
	}
	copyOff := e.org(t, ctx)
	e.armed(t, ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, copyOff)
	if s := e.snapshot(t, ctx, user, copyOff.String()).OrganizationStatus(copyOff.String()); s != authorization.StatusActive {
		t.Fatalf("a disabled auth copy of an active public organization: %v, want active (public wins)", s)
	}
	noCopy := e.publicOrg(t, ctx)
	if s := e.snapshot(t, ctx, user, noCopy.String()).OrganizationStatus(noCopy.String()); s != authorization.StatusActive {
		t.Fatalf("an active public organization with no auth copy: %v, want active", s)
	}
	unknown := uuid.NewString()
	if s := e.snapshot(t, ctx, user, unknown).OrganizationStatus(unknown); s != authorization.StatusUnknown {
		t.Fatalf("unknown organization: %v", s)
	}
	upper := "ABCDEF00-0000-4000-8000-000000000001"
	if s := e.snapshot(t, ctx, user, upper).OrganizationStatus(upper); s != authorization.StatusUnknown {
		t.Fatalf("non-canonical organization id: %v", s)
	}
}

func TestSessionLiveness(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user := e.user(t, ctx, true, false)
	seed := func(expires, revoked string) uuid.UUID {
		var id uuid.UUID
		if err := e.admin.QueryRow(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at, revoked_at, legacy_token_version)
VALUES ($1, $2, `+expires+`, `+revoked+`, 0) RETURNING id`, user, []byte(uuid.NewString())).Scan(&id); err != nil {
			t.Fatalf("seed session: %v", err)
		}
		return id
	}
	cases := map[string]struct {
		id          uuid.UUID
		found, live bool
	}{
		"live":    {seed("now() + interval '1 hour'", "NULL"), true, true},
		"expired": {seed("now() - interval '1 second'", "NULL"), true, false},
		"revoked": {seed("now() + interval '1 hour'", "now()"), true, false},
		"absent":  {uuid.New(), false, false},
	}
	for name, c := range cases {
		var s authapi.DecisionSession
		if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
			var err error
			s, err = tx.Session(ctx, c.id)
			return err
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if s.Found != c.found || s.Live != c.live || (c.found && s.PrincipalID != user) {
			t.Errorf("%s: %+v, want found %v live %v", name, s, c.found, c.live)
		}
	}
}

func TestReadIsOneReadOnlyRepeatableReadTransaction(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'member', now())`, uuid.New(), user, org)
	var first, second authorization.GrantSnapshot
	if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		if first, err = tx.Snapshot(ctx, user, org.String()); err != nil {
			return err
		}
		e.exec(t, ctx, `UPDATE public.memberships SET role = 'owner' WHERE user_id = $1 AND org_id = $2`, user, org)
		second, err = tx.Snapshot(ctx, user, org.String())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a, _ := first.Membership(org.String())
	b, _ := second.Membership(org.String())
	if a.Role != "member" || b.Role != "member" || first.NowUnixNano() != second.NowUnixNano() {
		t.Fatalf("a write committed during the transaction was seen: %q then %q", a.Role, b.Role)
	}
	if b := e.snapshot(t, ctx, user, org.String()); func() string { m, _ := b.Membership(org.String()); return m.Role }() != "owner" {
		t.Fatal("the next call did not see the committed write")
	}
}

func TestReadFailureIsAStoreError(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.reader.Read(ctx, func(authapi.DecisionTx) error { return nil })
	if !errors.Is(err, ErrStore) {
		t.Fatalf("Read on a cancelled context: %v, want ErrStore", err)
	}
}

func TestSessionReadsTheDelegationColumns(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	target, actor, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	var base, delegated uuid.UUID
	if err := e.admin.QueryRow(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at, legacy_token_version)
VALUES ($1, $2, now() + interval '1 hour', 0) RETURNING id`, actor, []byte(uuid.NewString())).Scan(&base); err != nil {
		t.Fatalf("seed base session: %v", err)
	}
	if err := e.admin.QueryRow(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at, legacy_token_version,
organization_id, actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
VALUES ($1, $2, now() + interval '10 minutes', 0, $3, $4, $5, 'support', ARRAY['analytics.read']) RETURNING id`,
		target, []byte(uuid.NewString()), org, actor, base).Scan(&delegated); err != nil {
		t.Fatalf("seed delegated session: %v", err)
	}
	for name, c := range map[string]struct {
		id        uuid.UUID
		delegated bool
	}{"plain": {base, false}, "delegated": {delegated, true}} {
		var s authapi.DecisionSession
		if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
			var err error
			s, err = tx.Session(ctx, c.id)
			return err
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !s.Found || !s.Live || s.HasActor != c.delegated || s.HasDelegatedFrom != c.delegated {
			t.Errorf("%s: %+v, want delegated %v", name, s, c.delegated)
		}
	}
}

// session seeds a session row of principal. delegatedFrom and actor make it an
// impersonation row of org.
func (e *env) session(t *testing.T, ctx context.Context, principal uuid.UUID, expiresIn string, revoked bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	revokedAt := "NULL"
	if revoked {
		revokedAt = "now()"
	}
	e.armed(t, ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, auth_time, amr, legacy_token_version, revoked_at)
VALUES ($1, $2, $3, now() + interval '`+expiresIn+`', now(), ARRAY['pwd'], 0, `+revokedAt+`)`, id, principal, []byte("ds-"+id.String()))
	return id
}

func (e *env) delegatedSession(t *testing.T, ctx context.Context, target, actor, base, org uuid.UUID, permitted []string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.armed(t, ctx, `INSERT INTO auth.sessions (id, principal_id, organization_id, token_hash, expires_at, auth_time, amr, legacy_token_version,
	actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
VALUES ($1, $2, $3, $4, now() + interval '10 minutes', now(), ARRAY['pwd','otp'], 0, $5, $6, 'support', $7)`,
		id, target, org, []byte("ds-"+id.String()), actor, base, permitted)
	return id
}

func (e *env) readSession(t *testing.T, ctx context.Context, id uuid.UUID) authapi.DecisionSession {
	t.Helper()
	var s authapi.DecisionSession
	if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		s, err = tx.Session(ctx, id)
		return err
	}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	return s
}

func TestSessionReadsTheDelegationColumnsAndTheBaseLiveness(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	target, actor, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	base := e.session(t, ctx, actor, "1 hour", false)
	id := e.delegatedSession(t, ctx, target, actor, base, org, []string{"git.read", "analytics.read"})
	s := e.readSession(t, ctx, id)
	if !s.Found || !s.Live || !s.HasActor || !s.HasDelegatedFrom || !s.BaseLive || s.ActorPrincipalID != actor ||
		s.OrganizationID == nil || *s.OrganizationID != org || len(s.AMR) != 2 || s.AMR[0] != "pwd" ||
		len(s.PermittedActions) != 2 || s.PermittedActions[0] != "git.read" {
		t.Fatalf("%+v", s)
	}
	plain := e.readSession(t, ctx, base)
	if !plain.Found || plain.HasActor || plain.HasDelegatedFrom || plain.BaseLive || plain.ActorPrincipalID != uuid.Nil || plain.OrganizationID != nil {
		t.Fatalf("a plain row: %+v", plain)
	}
	e.armed(t, ctx, `UPDATE auth.sessions SET revoked_at = now() WHERE id = $1`, base)
	if s := e.readSession(t, ctx, id); s.BaseLive || !s.Live {
		t.Fatalf("a revoked base: %+v, want the row live and the base not", s)
	}
	otherActor := e.user(t, ctx, true, false)
	expiredBase := e.session(t, ctx, otherActor, "-1 minute", false)
	if s := e.readSession(t, ctx, e.delegatedSession(t, ctx, target, otherActor, expiredBase, org, []string{"git.read"})); s.BaseLive {
		t.Fatalf("an expired base read as live: %+v", s)
	}
}

func (e *env) role(t *testing.T, ctx context.Context, principal uuid.UUID, role string) {
	t.Helper()
	e.armed(t, ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key) VALUES ($1, $2)`, principal, role)
}

func (e *env) actor(t *testing.T, ctx context.Context, actor uuid.UUID) authapi.ActorState {
	t.Helper()
	var a authapi.ActorState
	if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
		var err error
		a, err = tx.Actor(ctx, actor)
		return err
	}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	return a
}

func TestActorStateIsTheLiveRoleAndUserState(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	none := e.user(t, ctx, true, false)
	if a := e.actor(t, ctx, none); !a.Active || a.CanImpersonate || a.Owner {
		t.Fatalf("no role: %+v", a)
	}
	admin := e.user(t, ctx, true, false)
	e.role(t, ctx, admin, "platform_admin")
	if a := e.actor(t, ctx, admin); !a.Active || !a.CanImpersonate || a.Owner {
		t.Fatalf("platform_admin: %+v", a)
	}
	owner := e.user(t, ctx, true, false)
	e.role(t, ctx, owner, "platform_admin")
	e.role(t, ctx, owner, "platform_owner")
	if a := e.actor(t, ctx, owner); !a.Active || !a.CanImpersonate || !a.Owner {
		t.Fatalf("owner: %+v", a)
	}
	if a := e.actor(t, ctx, uuid.New()); a != (authapi.ActorState{}) {
		t.Fatalf("unknown principal: %+v", a)
	}
}

func TestActorDeactivatedUserIsNotActiveWhateverItsRole(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	inactive := e.user(t, ctx, false, false)
	e.role(t, ctx, inactive, "platform_admin")
	if a := e.actor(t, ctx, inactive); a.Active {
		t.Fatalf("users.is_active false read as active: %+v", a)
	}
	disabledCopy := e.user(t, ctx, true, false)
	e.role(t, ctx, disabledCopy, "platform_admin")
	e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, disabledCopy)
	if a := e.actor(t, ctx, disabledCopy); !a.Active || !a.CanImpersonate {
		t.Fatalf("a disabled auth copy of an active public user: %+v, want active with the role (public wins)", a)
	}
}

// An actor with a public.users row and an auth assignment but no auth.principals row is read as the role holder.
func TestActorWithNoAuthCopyHoldsItsRole(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	actor := e.publicUser(t, ctx, true, false)
	e.role(t, ctx, actor, "platform_admin")
	if a := e.actor(t, ctx, actor); !a.Active || !a.CanImpersonate || a.Owner {
		t.Fatalf("no auth copy: %+v, want an active admin", a)
	}
	e.role(t, ctx, actor, "platform_owner")
	if a := e.actor(t, ctx, actor); !a.Owner {
		t.Fatalf("no auth copy, owner assignment: %+v, want owner", a)
	}
	e.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, actor)
	if a := e.actor(t, ctx, actor); a.Active || a.CanImpersonate || a.Owner {
		t.Fatalf("public user inactive: %+v, want no authority", a)
	}
}

// The actor is read in the decision transaction: a role revoked after the
// transaction took its snapshot is still held inside it.
func TestActorIsReadInTheTransactionSnapshot(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	actor := e.user(t, ctx, true, false)
	e.role(t, ctx, actor, "platform_admin")
	if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
		if a, err := tx.Actor(ctx, actor); err != nil || !a.CanImpersonate {
			t.Fatalf("first read %+v %v", a, err)
		}
		e.armed(t, ctx, `DELETE FROM auth.platform_role_assignments WHERE principal_id = $1`, actor)
		a, err := tx.Actor(ctx, actor)
		if err != nil || !a.CanImpersonate {
			t.Fatalf("after the role was deleted outside the transaction: %+v %v, want the snapshot's state", a, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if a := e.actor(t, ctx, actor); a.CanImpersonate {
		t.Fatalf("a new transaction still holds the role: %+v", a)
	}
}

type delegatedSnap struct {
	authorization.GrantSnapshot
	del authorization.Delegation
}

func (d delegatedSnap) Delegation() (authorization.Delegation, bool) { return d.del, true }

// A real delegated row, its actor and the target's membership, read in one
// transaction and decided by the real evaluator.
func TestDelegatedRowDecidesOverTheRealRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	target, actor, org := e.user(t, ctx, true, false), e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'admin', now())`, uuid.New(), target, org)
	e.role(t, ctx, actor, "platform_admin")
	base := e.session(t, ctx, actor, "1 hour", false)
	id := e.delegatedSession(t, ctx, target, actor, base, org, []string{"analytics.read", "settings.write"})
	decide := func(action string) authorization.Decision {
		var d authorization.Decision
		if err := e.reader.Read(ctx, func(tx authapi.DecisionTx) error {
			s, err := tx.Session(ctx, id)
			if err != nil {
				return err
			}
			a, err := tx.Actor(ctx, s.ActorPrincipalID)
			if err != nil {
				return err
			}
			snap, err := tx.Snapshot(ctx, s.PrincipalID, org.String())
			if err != nil {
				return err
			}
			d = authorization.NewEvaluator().Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: org.String()},
				delegatedSnap{snap, delegationinput.Delegation(delegationinput.Facts{
					Actor: s.ActorPrincipalID, Organization: s.OrganizationID, AMR: s.AMR, PermittedActions: s.PermittedActions,
					ActorMayImpersonate: a.Active && a.CanImpersonate, ActorOwner: a.Owner,
				})})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	if d := decide("analytics.read"); !d.Allowed || d.Obligations == nil || d.Obligations.TenantScope != org.String() || d.Obligations.MinimumAssurance != "aal1" {
		t.Fatalf("permitted action: %+v %+v", d, d.Obligations)
	}
	if d := decide("settings.write"); d.Allowed || d.Reason != authorization.ReasonImpersonationDenied {
		t.Fatalf("admin actor on a denied action: %+v", d)
	}
	e.armed(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, actor)
	if d := decide("analytics.read"); d.Allowed || d.Reason != authorization.ReasonDelegationInvalid {
		t.Fatalf("deactivated actor: %+v", d)
	}
}
