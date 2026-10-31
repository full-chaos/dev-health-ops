//go:build integration

package grant

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

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	members  *membership.Store
	store    *Store
	notOwned *Store
	identity *identity.Store
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
	role, err := containers.RoleName("grant_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "grant-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
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
	members, err := membership.New(membership.Config{Pool: runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		return nil, err
	}
	store, err := New(Config{Pool: runtime, Schema: schema, OwnsUsers: true, Members: members})
	if err != nil {
		return nil, err
	}
	notOwned, err := New(Config{Pool: runtime, Schema: schema, Members: members})
	if err != nil {
		return nil, err
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, members: members, store: store, notOwned: notOwned, identity: id}, nil
}

func allowAll(context.Context, audit.TxOps, Locked) error { return nil }

func (e *env) seedUser(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, id, "grant-"+id.String()[:8]+"@example.test"); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'grant-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) member(t *testing.T, ctx context.Context, org uuid.UUID, role string) uuid.UUID {
	t.Helper()
	user := e.seedUser(t, ctx)
	if _, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: role, Authorize: allowAll}); err != nil {
		t.Fatalf("Add %s: %v", role, err)
	}
	return user
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) audits(t *testing.T, ctx context.Context, eventType string, org uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`, eventType, org.String())
}

func (e *env) events(t *testing.T, ctx context.Context, eventType string, id uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = $1 AND aggregate_id = $2`, eventType, "grt_"+id.String())
}

type setup struct {
	org, admin, user uuid.UUID
}

func (e *env) setup(t *testing.T, ctx context.Context) setup {
	org := e.seedOrg(t, ctx)
	owner := e.member(t, ctx, org, "owner")
	return setup{org: org, admin: owner, user: e.member(t, ctx, org, "member")}
}

func (e *env) create(t *testing.T, ctx context.Context, s setup, role, key string) *Grant {
	t.Helper()
	actor := s.admin
	g, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: role, Key: key, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil {
		t.Fatalf("Create %s %s: %v", role, key, err)
	}
	return g
}

// I1: create, duplicate refused, revoke, re-grant is a new row; both rows stay.
func TestCreateRevokeRegrantKeepsHistory(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "viewer", "acme/api")
	if g.Revision != 1 || g.Origin != OriginAPI || g.GrantedBy == nil || *g.GrantedBy != s.admin || !g.Live() {
		t.Fatalf("created grant = %+v", g)
	}
	if n := e.audits(t, ctx, "auth.resource_grant.created", s.org); n != 1 {
		t.Fatalf("created audit rows = %d, want 1", n)
	}
	if n := e.events(t, ctx, "resource_grant.created", g.ID); n != 1 {
		t.Fatalf("created outbox rows = %d, want 1", n)
	}
	actor := s.admin
	if _, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: "acme/api", Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("duplicate Create = %v, want ErrAlreadyGranted", err)
	}
	r, err := e.store.Revoke(ctx, RevokeInput{OrgID: s.org, GrantID: g.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if r.Live() || r.RevokedBy == nil || *r.RevokedBy != s.admin || r.Revision != 2 {
		t.Fatalf("revoked grant = %+v", r)
	}
	if n := e.events(t, ctx, "resource_grant.revoked", g.ID); n != 1 {
		t.Fatalf("revoked outbox rows = %d, want 1", n)
	}
	g2 := e.create(t, ctx, s, "viewer", "acme/api")
	if g2.ID == g.ID {
		t.Fatal("re-grant reused the revoked row")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE organization_id = $1 AND principal_id = $2`, s.org, s.user); n != 2 {
		t.Fatalf("rows = %d, want 2 (history kept)", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE id = $1 AND granted_by = $2 AND revoked_by = $2`, g.ID, s.admin); n != 1 {
		t.Fatal("the revoked row lost its lineage")
	}
}

// I10: a second Revoke changes nothing and writes no second event.
func TestRevokeIsIdempotentAndMonotonic(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "member", "*")
	actor := s.admin
	in := RevokeInput{OrgID: s.org, GrantID: g.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}
	if _, err := e.store.Revoke(ctx, in); err != nil {
		t.Fatal(err)
	}
	again, err := e.store.Revoke(ctx, in)
	if err != nil || again.Live() || again.Revision != 2 {
		t.Fatalf("second Revoke = %+v, %v; want unchanged revision 2", again, err)
	}
	if n := e.events(t, ctx, "resource_grant.revoked", g.ID); n != 1 {
		t.Fatalf("revoked outbox rows = %d, want 1", n)
	}
	var wg sync.WaitGroup
	g3 := e.create(t, ctx, s, "admin", "acme/web")
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.store.Revoke(ctx, RevokeInput{OrgID: s.org, GrantID: g3.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Revoke: %v", err)
		}
	}
	if n := e.events(t, ctx, "resource_grant.revoked", g3.ID); n != 1 {
		t.Fatalf("concurrent revoked outbox rows = %d, want 1", n)
	}
}

// I2: a revoked grant is not updated, by the Store or by SQL.
func TestUpdateAfterRevokeIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "viewer", "acme/api")
	actor := s.admin
	if _, err := e.store.Revoke(ctx, RevokeInput{OrgID: s.org, GrantID: g.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	role := "admin"
	if _, err := e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, Role: &role, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Update after revoke = %v, want ErrRevoked", err)
	}
	for _, sql := range []string{
		`UPDATE auth.resource_grants SET revoked_at = NULL WHERE id = $1`,
		`UPDATE auth.resource_grants SET origin = 'backfill' WHERE id = $1`,
		`UPDATE auth.resource_grants SET role_key = 'admin' WHERE id = $1`,
	} {
		if _, err := e.admin.Exec(ctx, sql, g.ID); err == nil {
			t.Fatalf("%s: accepted on a revoked grant", sql)
		}
	}
}

func TestUpdateChangesRoleAndExpiryOnly(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "viewer", "acme/api")
	actor := s.admin
	role, soon := "admin", time.Now().Add(time.Hour)
	u, err := e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, Role: &role, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil || u.Role != "admin" || u.Revision != 2 || u.Key != "acme/api" {
		t.Fatalf("Update role = %+v, %v", u, err)
	}
	u, err = e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, ExpiresAt: &soon, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil || u.ExpiresAt == nil || u.Revision != 3 {
		t.Fatalf("Update expiry = %+v, %v", u, err)
	}
	same, err := e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, Role: &role, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil || same.Revision != 3 {
		t.Fatalf("no-op Update = %+v, %v; want unchanged revision 3", same, err)
	}
	cleared, err := e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, ClearExpiry: true, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil || cleared.ExpiresAt != nil {
		t.Fatalf("clear expiry = %+v, %v", cleared, err)
	}
	stale := int64(1)
	if _, err := e.store.Update(ctx, UpdateInput{OrgID: s.org, GrantID: g.ID, Role: &role, ExpectedRevision: &stale, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale revision = %v, want ErrRevisionConflict", err)
	}
}

// I3: the target must be an active member of the same organization.
func TestTargetMustBeAnActiveMemberOfTheOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	actor := s.admin
	other := e.seedOrg(t, ctx)
	outsider := e.seedUser(t, ctx)
	for name, in := range map[string]CreateInput{
		"no membership":    {OrgID: s.org, PrincipalID: outsider},
		"other org member": {OrgID: other, PrincipalID: s.user},
	} {
		in.Role, in.Key, in.Actor, in.Authorize = "viewer", "acme/api", Actor{PrincipalID: &actor}, allowAll
		if _, err := e.store.Create(ctx, in); !errors.Is(err, ErrNotMember) {
			t.Errorf("%s: Create = %v, want ErrNotMember", name, err)
		}
	}
	if _, err := e.members.Suspend(ctx, membership.Change{OrgID: s.org, PrincipalID: s.user, Authorize: func(context.Context, audit.TxOps, membership.Locked) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: "acme/api", Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrNotMember) {
		t.Errorf("suspended member: Create = %v, want ErrNotMember", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE principal_id IN ($1, $2)`, outsider, s.user); n != 0 {
		t.Fatalf("rows written by refused creates = %d", n)
	}
}

// The same key in two organizations is two unrelated grants.
func TestSameKeyInTwoOrganizationsIsIndependent(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	a, b := e.setup(t, ctx), e.setup(t, ctx)
	ga, gb := e.create(t, ctx, a, "viewer", "acme/api"), e.create(t, ctx, b, "viewer", "acme/api")
	actor := a.admin
	if _, err := e.store.Revoke(ctx, RevokeInput{OrgID: a.org, GrantID: ga.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if got, err := e.store.Get(ctx, b.org, gb.ID); err != nil || !got.Live() {
		t.Fatalf("grant of the other organization = %+v, %v", got, err)
	}
	if _, err := e.store.Get(ctx, a.org, gb.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get across organizations = %v, want ErrNotFound", err)
	}
	if _, err := e.store.Revoke(ctx, RevokeInput{OrgID: a.org, GrantID: gb.ID, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Revoke across organizations = %v, want ErrNotFound", err)
	}
}

// I4: an expiry must be in the future; none is fine.
func TestExpiryMustBeInTheFuture(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	actor := s.admin
	past, future := time.Now().Add(-time.Minute), time.Now().Add(400*24*time.Hour)
	if _, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: "acme/api", ExpiresAt: &past, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrExpiryPast) {
		t.Fatalf("past expiry = %v, want ErrExpiryPast", err)
	}
	g, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: "acme/api", ExpiresAt: &future, Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
	if err != nil || g.ExpiresAt == nil {
		t.Fatalf("future expiry (over a year) = %+v, %v; no horizon applies", g, err)
	}
}

// A refused Authorize and a nil Authorize write nothing, and an unauthorized
// actor learns nothing about which grants exist.
func TestAuthorizeRefusalWritesNothingAndDisclosesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "viewer", "acme/api")
	actor := s.user
	deny := func(context.Context, audit.TxOps, Locked) error { return ErrDenied }
	if _, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "admin", Key: "x/y", Actor: Actor{PrincipalID: &actor}, Authorize: deny}); !errors.Is(err, ErrDenied) {
		t.Fatalf("denied Create = %v", err)
	}
	if _, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "admin", Key: "x/y", Actor: Actor{PrincipalID: &actor}}); !errors.Is(err, ErrDenied) {
		t.Fatalf("nil Authorize Create = %v", err)
	}
	for _, id := range []uuid.UUID{g.ID, uuid.New()} {
		if _, err := e.store.Revoke(ctx, RevokeInput{OrgID: s.org, GrantID: id, Actor: Actor{PrincipalID: &actor}, Authorize: deny}); !errors.Is(err, ErrDenied) {
			t.Fatalf("denied Revoke of %s = %v, want ErrDenied for a real and a missing grant alike", id, err)
		}
	}
	if _, err := e.store.Revoke(ctx, RevokeInput{OrgID: s.org, GrantID: uuid.New(), Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("authorized Revoke of a missing grant = %v, want ErrNotFound", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE organization_id = $1`, s.org); n != 1 {
		t.Fatalf("rows = %d, want only the first grant", n)
	}
}

// With users not owned every verb refuses (U7's database twin).
func TestVerbsRefuseWhenUsersAreNotOwned(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	actor := s.admin
	if _, err := e.notOwned.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: "a/b", Actor: Actor{PrincipalID: &actor}, Authorize: allowAll}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Create = %v, want ErrNotOwned", err)
	}
}

// I9: the runtime role cannot delete a grant; the commit guard refuses a raw write.
func TestRuntimeRoleCannotDeleteGrants(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	s := e.setup(t, ctx)
	g := e.create(t, ctx, s, "viewer", "acme/api")
	if _, err := e.runtime.Exec(ctx, `DELETE FROM auth.resource_grants WHERE id = $1`, g.ID); err == nil {
		t.Fatal("the runtime role deleted a grant")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.resource_grants WHERE id = $1`, g.ID); n != 1 {
		t.Fatal("grant row gone")
	}
}

// I7: Remove against Create on one pair never deadlocks and ends consistent.
func TestRemoveAndCreateOnOnePairDoNotDeadlock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s := e.setup(t, ctx)
	actor := s.admin
	for i := range 50 {
		errs := make(chan error, 2)
		go func() {
			_, err := e.members.Remove(ctx, membership.Change{OrgID: s.org, PrincipalID: s.user, Actor: membership.Actor{PrincipalID: &actor}, Authorize: allowAll})
			if errors.Is(err, membership.ErrNotMember) {
				err = nil
			}
			errs <- err
		}()
		go func() {
			_, err := e.store.Create(ctx, CreateInput{OrgID: s.org, PrincipalID: s.user, Role: "viewer", Key: fmt.Sprintf("acme/r%d", i), Actor: Actor{PrincipalID: &actor}, Authorize: allowAll})
			if errors.Is(err, ErrNotMember) {
				err = nil
			}
			errs <- err
		}()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: %v", i, err)
			}
		}
		// Whatever order won, a removed member holds no live grant.
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships m JOIN auth.resource_grants g
ON g.organization_id = m.organization_id AND g.principal_id = m.principal_id
WHERE m.organization_id = $1 AND m.principal_id = $2 AND m.status <> 'active' AND g.revoked_at IS NULL`, s.org, s.user); n != 0 {
			t.Fatalf("round %d: %d live grants of a member who is not active", i, n)
		}
		if _, err := e.members.Add(ctx, membership.Change{OrgID: s.org, PrincipalID: s.user, Role: "member", Authorize: allowAll}); err != nil && !errors.Is(err, membership.ErrAlreadyMember) {
			t.Fatalf("round %d re-add: %v", i, err)
		}
	}
}
