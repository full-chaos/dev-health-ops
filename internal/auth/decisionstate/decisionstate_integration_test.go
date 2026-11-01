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
// transaction that arms the audit commit guard.
func (e *env) armed(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true),
		set_config('dev_health.audit_changed', pg_current_xact_id()::text || ':0', true)`); err != nil {
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

func TestSnapshotActiveAuthRowWithoutAPublicRowIsRemoved(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, joined_at) VALUES ($1, $2, 'active', 'admin', now())`, org, user)
	m, ok := e.snapshot(t, ctx, user, org.String()).Membership(org.String())
	if !ok || m.Status != authorization.StatusRemoved {
		t.Fatalf("membership %+v %v, want removed", m, ok)
	}
	d := authorization.NewEvaluator().Evaluate(authorization.Request{Action: "settings.write", ResourceKind: "organization", OrganizationID: org.String()},
		e.snapshot(t, ctx, user, org.String()))
	if d.Allowed || d.Reason != authorization.ReasonMembershipInactive {
		t.Fatalf("decision %+v, want membership_inactive", d)
	}
}

func TestSnapshotPrincipalNeedsBothFlagsAndTheUserKind(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	inactive := e.user(t, ctx, false, false)
	if s := e.snapshot(t, ctx, inactive, "").PrincipalStatus(); s == authorization.StatusActive {
		t.Fatal("users.is_active false read as active")
	}
	disabled := e.user(t, ctx, true, false)
	e.armed(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, disabled)
	if s := e.snapshot(t, ctx, disabled, "").PrincipalStatus(); s == authorization.StatusActive {
		t.Fatal("a disabled principal read as active")
	}
	noUsersRow := uuid.New()
	e.armed(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, noUsersRow)
	if s := e.snapshot(t, ctx, noUsersRow, "").PrincipalStatus(); s != authorization.StatusUnknown {
		t.Fatalf("a user principal with no public.users row: %v, want unknown", s)
	}
	for _, kind := range []string{"service_account", "workload"} {
		id := uuid.New()
		e.armed(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, $2)`, id, kind)
		if s := e.snapshot(t, ctx, id, "").PrincipalStatus(); s != authorization.StatusUnknown {
			t.Fatalf("kind %s: %v, want unknown", kind, s)
		}
	}
	// A non-user principal whose id also has a public.users row is still not
	// a user: the kind decides, not the row.
	shadow := uuid.New()
	e.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, true, 0, now(), now())`, shadow, "shadow-"+shadow.String()[:8]+"@example.com")
	e.armed(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'service_account')`, shadow)
	if snap := e.snapshot(t, ctx, shadow, ""); snap.PrincipalStatus() != authorization.StatusUnknown || snap.PlatformRole() != "" {
		t.Fatalf("a service account with a users row: %v %q, want unknown and no platform role", snap.PrincipalStatus(), snap.PlatformRole())
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
	e.armed(t, ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, org)
	if s := e.snapshot(t, ctx, user, org.String()).OrganizationStatus(org.String()); s != authorization.StatusSuspended {
		t.Fatalf("disabled organization: %v", s)
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
