// Package platformroleseam gives the integration tests of the platform role
// read seam one real database: the migrated ops schema, the auth schema with
// a session reader role (go-api's), and users whose public is_superuser and
// auth assignment are set independently.
package platformroleseam

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const password = "platformroleseam-test-only-not-a-credential"

// Env is one Postgres with both schemas. Reader is connected as the session
// reader role, which also holds SELECT on public.users and public.memberships
// (go-api's reads); Admin is the superuser.
type Env struct {
	URI         string
	Admin       *pgxpool.Pool
	Reader      *pgxpool.Pool
	ReaderRole  string
	RuntimeRole string
	Schema      authschema.ValidatedIdentifier
	store       *identity.Store
}

// Start builds the database; owned is the reader grant's platform roles switch.
func Start(ctx context.Context, t testing.TB, owned bool) *Env {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(c)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	e := &Env{URI: instance.URI, Admin: admin}
	for _, r := range []struct {
		name *string
		base string
	}{{&e.RuntimeRole, "prs_runtime"}, {&e.ReaderRole, "prs_reader"}} {
		role, err := containers.RoleName(r.base, instance)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		*r.name = role
	}
	if e.Schema, err = authschema.NewValidatedIdentifier("auth"); err != nil {
		t.Fatal(err)
	}
	e.Apply(ctx, t, owned, "")
	if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT ON public.users, public.memberships TO %q`, e.ReaderRole)); err != nil {
		t.Fatal(err)
	}
	e.Reader = e.ConnectAs(ctx, t, e.ReaderRole)
	if e.store, err = identity.New(identity.Config{Pool: admin, Schema: e.Schema}); err != nil {
		t.Fatal(err)
	}
	return e
}

// Apply runs auth-migrate's apply with the reader role and the switch.
func (e *Env) Apply(ctx context.Context, t testing.TB, owned bool, ack string) {
	t.Helper()
	if err := e.TryApply(ctx, owned, ack, false); err != nil {
		t.Fatalf("authschema.Apply (owned %v): %v", owned, err)
	}
}

// TryApply is Apply that returns the error.
func (e *Env) TryApply(ctx context.Context, owned bool, ack string, deferNarrowing bool) error {
	_, err := authschema.Apply(ctx, e.Admin, authschema.Options{
		Schema: "auth", RuntimeRole: e.RuntimeRole, SessionReaderRoles: []string{e.ReaderRole},
		PlatformRolesOwned: owned, AcknowledgeNarrowing: ack, DeferNarrowing: deferNarrowing,
	})
	return err
}

// ConnectAs opens a pool as one of the Env's roles.
func (e *Env) ConnectAs(ctx context.Context, t testing.TB, role string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(e.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// User inserts a public user (is_superuser = publicSuperuser) and its auth
// principal; assigned gives the principal the no-granter platform_admin
// assignment, whatever public says.
func (e *Env) User(ctx context.Context, t testing.TB, publicSuperuser, assigned bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	pgseed.User(ctx, t, e.Admin, id.String())
	return e.Project(ctx, t, id, publicSuperuser, assigned)
}

// Project sets an existing public user's is_superuser (the proof principals
// are seeded by the migrated schema) and projects its auth principal, with the
// no-granter platform_admin assignment when assigned.
func (e *Env) Project(ctx context.Context, t testing.TB, id uuid.UUID, publicSuperuser, assigned bool) uuid.UUID {
	t.Helper()
	pgseed.SetUserState(ctx, t, e.Admin, id.String(), true, publicSuperuser, 0)
	if _, err := e.store.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: e.email(ctx, t, id), Active: true, Verified: true, Superuser: assigned}, false); err != nil {
		t.Fatalf("project the auth principal: %v", err)
	}
	return id
}

func (e *Env) email(ctx context.Context, t testing.TB, id uuid.UUID) string {
	t.Helper()
	var email string
	if err := e.Admin.QueryRow(ctx, `SELECT email FROM public.users WHERE id = $1`, id).Scan(&email); err != nil {
		t.Fatal(err)
	}
	return email
}

// Disable disables the user's auth principal (public is left as it is).
func (e *Env) Disable(ctx context.Context, t testing.TB, id uuid.UUID) {
	t.Helper()
	var assigned bool
	if err := e.Admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.platform_role_assignments WHERE principal_id = $1)`, id).Scan(&assigned); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: e.email(ctx, t, id), Active: false, Verified: true, Superuser: assigned}, false); err != nil {
		t.Fatalf("disable the auth principal: %v", err)
	}
}

// ExpiredUser is User whose only assignment was granted (through the platform
// role store, by an administrator of its own) with an expiry that has passed
// on the database clock.
func (e *Env) ExpiredUser(ctx context.Context, t testing.TB, publicSuperuser bool) uuid.UUID {
	t.Helper()
	id := e.User(ctx, t, publicSuperuser, false)
	granter := e.User(ctx, t, false, true)
	roles, err := platformrole.New(platformrole.Config{Pool: e.Admin, Schema: e.Schema})
	if err != nil {
		t.Fatal(err)
	}
	var expires time.Time
	if err := e.Admin.QueryRow(ctx, `SELECT statement_timestamp() + interval '2 seconds'`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if _, err := roles.Grant(ctx, platformrole.GrantInput{Actor: granter, Principal: id, Role: platformrole.PlatformAdmin, ExpiresAt: &expires}); err != nil {
		t.Fatalf("grant an expiring assignment: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var passed bool
		if err := e.Admin.QueryRow(ctx, `SELECT statement_timestamp() > $1`, expires).Scan(&passed); err != nil {
			t.Fatal(err)
		}
		if passed {
			return id
		}
		if time.Now().After(deadline) {
			t.Fatal("the assignment did not expire on the database clock")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Member puts the user in a new organization with role and returns the
// organization's id.
func (e *Env) Member(ctx context.Context, t testing.TB, user uuid.UUID, role string) uuid.UUID {
	t.Helper()
	org := uuid.New()
	pgseed.Org(ctx, t, e.Admin, org.String(), "free")
	pgseed.Membership(ctx, t, e.Admin, uuid.NewString(), user.String(), org.String(), role)
	return org
}
