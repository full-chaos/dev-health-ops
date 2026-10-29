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

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
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
	return e.UserWithID(ctx, t, uuid.New(), "local", publicSuperuser, assigned)
}

// UserWithID is User with the id and the public auth_provider given. A public
// row the migrated schema already seeds (the proof principals) keeps its
// email and gets is_superuser and auth_provider.
func (e *Env) UserWithID(ctx context.Context, t testing.TB, id uuid.UUID, provider string, publicSuperuser, assigned bool) uuid.UUID {
	t.Helper()
	email := fmt.Sprintf("seam-%s@example.test", id.String()[:8])
	if _, err := e.Admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, $4, true, true, $3, 0, now(), now())
ON CONFLICT (id) DO UPDATE SET is_superuser = EXCLUDED.is_superuser, auth_provider = EXCLUDED.auth_provider`, id, email, publicSuperuser, provider); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if _, err := e.store.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: email, Active: true, Verified: true, Superuser: assigned}, false); err != nil {
		t.Fatalf("project the auth principal: %v", err)
	}
	return id
}

// Disable disables the user's auth principal (public is left as it is).
func (e *Env) Disable(ctx context.Context, t testing.TB, id uuid.UUID) {
	t.Helper()
	var email string
	if err := e.Admin.QueryRow(ctx, `SELECT email FROM public.users WHERE id = $1`, id).Scan(&email); err != nil {
		t.Fatal(err)
	}
	var assigned bool
	if err := e.Admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM auth.platform_role_assignments WHERE principal_id = $1)`, id).Scan(&assigned); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: email, Active: false, Verified: true, Superuser: assigned}, false); err != nil {
		t.Fatalf("disable the auth principal: %v", err)
	}
}

// Expire replaces the user's assignment by one that expired a minute ago.
func (e *Env) Expire(ctx context.Context, t testing.TB, id uuid.UUID) {
	t.Helper()
	var old uuid.UUID
	if err := e.Admin.QueryRow(ctx, `SELECT id FROM auth.platform_role_assignments WHERE principal_id = $1`, id).Scan(&old); err != nil {
		t.Fatal(err)
	}
	expired := uuid.New()
	subject := id.String()
	err := audit.Commit(ctx, e.Admin, e.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `DELETE FROM auth.platform_role_assignments WHERE id = $1`, old); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, expires_at)
VALUES ($1, $2, 'platform_admin', statement_timestamp() - interval '1 minute')`, expired, id)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{
			{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", old.String()),
				EventType: "platform_role_assignment.revoked", Change: map[string]any{"status": "revoked"}},
			{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", expired.String()),
				EventType: "platform_role_assignment.granted", Change: map[string]any{"status": "active"}},
		},
	})
	if err != nil {
		t.Fatalf("expire the assignment: %v", err)
	}
}

// Member puts the user in a new organization with role and returns the
// organization's id.
func (e *Env) Member(ctx context.Context, t testing.TB, user uuid.UUID, role string) uuid.UUID {
	t.Helper()
	org := uuid.New()
	if _, err := e.Admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1, $2, 'seam', 'free', true, now(), now())`, org, "seam-"+org.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if _, err := e.Admin.Exec(ctx, `INSERT INTO public.memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), org, user, role); err != nil {
		t.Fatalf("seed public.memberships: %v", err)
	}
	return org
}
