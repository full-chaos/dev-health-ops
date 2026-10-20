//go:build integration

package social

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// identityColumns are the public.users columns the identity store reads.
const identityColumns = "id, email, password_hash, is_active, is_verified, auth_provider, auth_provider_id"

type env struct {
	admin     *pgxpool.Pool
	runtime   *pgxpool.Pool
	schema    authschema.ValidatedIdentifier
	stores    map[bool]*identity.Store
	providers *fakeProviders
	now       time.Time
}

// newEnv builds one PostgreSQL with the real public schema and the auth
// schema, and identity stores that connect AS the runtime role on its default
// search_path, holding the declared posture plus the public column grants.
func newEnv(t *testing.T, ctx context.Context) *env {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("social_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "social-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, grant := range []string{
		fmt.Sprintf(`GRANT SELECT (%s) ON public.users TO %q`, identityColumns, role),
		fmt.Sprintf(`GRANT SELECT (user_id, org_id) ON public.memberships TO %q`, role),
		fmt.Sprintf(`GRANT SELECT (id, org_id) ON public.sso_providers TO %q`, role),
		fmt.Sprintf(`GRANT SELECT (id, slug, name, is_active) ON public.organizations TO %q`, role),
	} {
		if _, err := admin.Exec(ctx, grant); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	e := &env{admin: admin, runtime: runtime, schema: schema, stores: map[bool]*identity.Store{},
		providers: newFakeProviders(t), now: time.Now()}
	for _, owns := range []bool{false, true} {
		store, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: owns})
		if err != nil {
			t.Fatal(err)
		}
		e.stores[owns] = store
	}
	return e
}

// store is the identity store; ownsUsers is its UsersOwnedByAuth.
func (e *env) store(ownsUsers bool) *identity.Store { return e.stores[ownsUsers] }

func (e *env) service(t *testing.T, ownsUsers bool) *Service {
	t.Helper()
	svc, err := New(Config{Store: e.store(ownsUsers), Providers: e.providers.client(), Now: func() time.Time { return e.now }})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func (e *env) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%q: %v", sql, err)
	}
}

// goAPIUser inserts a public.users row as go-api writes it. provider "" is a
// password account (auth_provider 'local').
func (e *env) goAPIUser(t *testing.T, ctx context.Context, email string, provider Provider, providerID string, active bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var hash, method, pid any = nil, "local", nil
	if provider == "" {
		hash = testVerifier(t)
	} else {
		method, pid = string(provider), providerID
	}
	e.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, true, false, 0, now(), now())`, id, email, hash, method, pid, active)
	return id
}

// localUser is a go-api password account, projected as a sign-in projects it.
func (e *env) localUser(t *testing.T, ctx context.Context, email string) uuid.UUID {
	t.Helper()
	id := e.goAPIUser(t, ctx, email, "", "", true)
	if err := e.store(false).Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

// link binds key to principal through the identity store, as an explicit link does.
func (e *env) link(t *testing.T, ctx context.Context, principal uuid.UUID, key identity.ExternalKey) uuid.UUID {
	t.Helper()
	id, err := e.store(false).LinkIdentity(ctx, key, principal, identity.Actor{})
	if err != nil {
		t.Fatalf("LinkIdentity: %v", err)
	}
	return id
}

// disable deactivates a go-api account and projects the change.
func (e *env) disable(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	e.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, id)
	if err := e.store(false).Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
}

// testVerifier is a bcrypt verifier of a random password, built at run time.
func testVerifier(t *testing.T) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(uuid.NewString()), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// state is the row count of every table an exchange or link can write.
type state struct{ principals, users, identities, audits, outbox int }

func (e *env) state(t *testing.T, ctx context.Context) state {
	t.Helper()
	return state{
		principals: e.count(t, ctx, `SELECT count(*) FROM auth.principals`),
		users:      e.count(t, ctx, `SELECT count(*) FROM auth.users`),
		identities: e.count(t, ctx, `SELECT count(*) FROM auth.external_identities`),
		audits:     e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`),
		outbox:     e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events`),
	}
}

func (e *env) unchanged(t *testing.T, ctx context.Context, before state) {
	t.Helper()
	if after := e.state(t, ctx); after != before {
		t.Fatalf("rows changed: before %+v, after %+v", before, after)
	}
}

func (e *env) identityOwner(t *testing.T, ctx context.Context, key identity.ExternalKey) (uuid.UUID, bool) {
	t.Helper()
	var owner uuid.UUID
	err := e.admin.QueryRow(ctx, `SELECT principal_id FROM auth.external_identities WHERE issuer = $1 AND provider = $2 AND subject = $3`,
		key.Issuer, string(key.Provider), key.Subject).Scan(&owner)
	if err != nil {
		return uuid.Nil, false
	}
	return owner, true
}
