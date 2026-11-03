//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const queryAPISessionsPassword = "query_api_sessions_password"

type queryAPISessionsHarness struct {
	admin *pgxpool.Pool
	uri   string
	roles grantRoleNames
	role  string
}

func startQueryAPISessionsHarness(t *testing.T, ctx context.Context, withAuthSchema bool) *queryAPISessionsHarness {
	t.Helper()
	admin, uri, roles := startGrantHarness(t, ctx)
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	createQueryAPIColumnScopedStandins(ctx, t, admin)
	role := roles.domain + "_s"
	if len(role) > 60 {
		role = role[:60]
	}
	var dbName string
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	statements := []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + queryAPISessionsPassword + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
	}
	if withAuthSchema {
		statements = append(statements,
			"CREATE SCHEMA auth",
			"CREATE TABLE auth.sessions (id uuid PRIMARY KEY, principal_id uuid, revoked_at timestamptz, legacy_token_version bigint, token_hash text)",
			"CREATE TABLE auth.principals (id uuid PRIMARY KEY, kind text, disabled_at timestamptz, display_name text)",
			"CREATE TABLE auth.platform_role_assignments (id uuid PRIMARY KEY, principal_id uuid, role_key text, expires_at timestamptz, granted_by uuid)",
		)
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	return &queryAPISessionsHarness{admin: admin, uri: uri, roles: roles, role: role}
}

func (h *queryAPISessionsHarness) applyLeg(ctx context.Context, sessions *QueryAPISessionRead) error {
	grants := make([]riverstore.TableGrant, 0)
	for _, table := range QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	columnGrants := make([]riverstore.ColumnGrant, 0)
	for _, column := range QueryAPIPosture().ColumnScoped {
		columnGrants = append(columnGrants, riverstore.ColumnGrant{TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege})
	}
	options := riverstore.MigrationOptions{
		Schema: grantSchema, DomainRole: h.roles.domain, QueueRole: h.roles.queue,
		QueryAPIRole: h.role, QueryAPIGrants: grants, QueryAPIColumnGrants: columnGrants,
	}
	if sessions != nil {
		grant := &riverstore.SessionReadGrant{Schema: sessions.Schema, Table: sessions.Table, Columns: sessions.Columns}
		for _, table := range sessions.Also {
			grant.Also = append(grant.Also, riverstore.SessionReadTable{Table: table.Table, Columns: table.Columns})
		}
		options.QueryAPISessionRead = grant
	}
	_, err := riverstore.ApplyPinnedMigrations(ctx, h.admin, options)
	return err
}

// auth-migrate's session-reader grant, built by the production builder and
// applied as the other writer of the same grant.
func (h *queryAPISessionsHarness) applyAuthMigrateGrant(t *testing.T, ctx context.Context) {
	t.Helper()
	h.applyAuthMigrateGrantOwned(t, ctx, false)
}

func (h *queryAPISessionsHarness) applyAuthMigrateGrantOwned(t *testing.T, ctx context.Context, platformRolesOwned bool) {
	t.Helper()
	statements, err := authschema.SessionReaderStatements(ctx, h.admin,
		authschema.Options{Schema: "auth", SessionReaderRoles: []string{h.role}, PlatformRolesOwned: platformRolesOwned})
	if err != nil {
		t.Fatalf("auth-migrate session reader statements: %v", err)
	}
	if len(statements) == 0 {
		t.Fatal("auth-migrate produced no session reader statements")
	}
	for _, statement := range statements {
		if _, err := h.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func (h *queryAPISessionsHarness) check(t *testing.T, ctx context.Context, sessions *QueryAPISessionRead) error {
	t.Helper()
	pool := connectAs(t, ctx, h.uri, h.role, queryAPISessionsPassword)
	return CheckQueryAPIAuthorizationWithSessions(ctx, pool, h.role, grantSchema, sessions)
}

// The river leg's REVOKE-then-GRANT ends with exactly the manifest set, and the
// auth.sessions read is in it when the flag is on. A privilege planted in the auth
// schema beforehand does not survive.
func TestQueryAPIMigrateLegEndsWithExactlyTheManifestPlusTheSessionRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, true)
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA auth TO " + h.role,
		"GRANT SELECT (token_hash) ON auth.sessions TO " + h.role,
		"GRANT SELECT ON auth.principals TO " + h.role,
	} {
		if _, err := h.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	read := QueryAPISessionReadFor("auth", false)
	if err := h.applyLeg(ctx, &read); err != nil {
		t.Fatalf("the migrate leg: %v", err)
	}
	if err := h.check(t, ctx, &read); err != nil {
		t.Fatalf("exactly the manifest plus the session read must be ready: %v", err)
	}
	if err := h.check(t, ctx, nil); err == nil || !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "outside the manifest") {
		t.Fatalf("with the flag off the session read is outside the manifest: %v", err)
	}
	if err := h.applyLeg(ctx, nil); err != nil {
		t.Fatalf("the migrate leg with the flag off: %v", err)
	}
	if err := h.check(t, ctx, nil); err != nil {
		t.Fatalf("flag off: the next run must remove the read: %v", err)
	}
}

// Backstop: the flag is on and the auth sessions table does not exist yet. The
// migration still succeeds, and readiness stays refused (fail closed) until a later run.
func TestQueryAPIMigrateLegSkipsTheSessionReadWhenTheAuthSessionsTableIsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, false)
	read := QueryAPISessionReadFor("auth", false)
	if err := h.applyLeg(ctx, &read); err != nil {
		t.Fatalf("an absent auth schema must not abort the migration: %v", err)
	}
	if err := h.check(t, ctx, &read); err == nil || !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "lacks") {
		t.Fatalf("readiness must stay refused naming the missing read: %v", err)
	}
	if err := h.check(t, ctx, nil); err != nil {
		t.Fatalf("the rest of the manifest must be in place: %v", err)
	}
}

// auth-migrate and the river leg write the same grant: either order, and a repeat,
// ends in the same ready state.
func TestQueryAPIPostureHoldsAfterAuthMigrateAndRiverMigrateInEitherOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, true)
	read := QueryAPISessionReadFor("auth", false)
	steps := []struct {
		name string
		run  func()
	}{
		{"auth-migrate", func() { h.applyAuthMigrateGrant(t, ctx) }},
		{"river-migrate", func() {
			if err := h.applyLeg(ctx, &read); err != nil {
				t.Fatalf("river leg: %v", err)
			}
		}},
	}
	for _, order := range [][]int{{0, 1}, {1, 0}, {0, 1, 1, 0}} {
		for _, i := range order {
			steps[i].run()
			if i == 1 {
				if err := h.check(t, ctx, &read); err != nil {
					t.Fatalf("after %v, step %s: %v", order, steps[i].name, err)
				}
			}
		}
		if err := h.check(t, ctx, &read); err != nil {
			t.Fatalf("order %v: %v", order, err)
		}
	}
}

// As the role: the session columns read, anything else in auth is refused (42501).
func TestQueryAPIRoleCannotReadOtherAuthColumnsOrTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, true)
	read := QueryAPISessionReadFor("auth", false)
	if err := h.applyLeg(ctx, &read); err != nil {
		t.Fatalf("the migrate leg: %v", err)
	}
	pool := connectAs(t, ctx, h.uri, h.role, queryAPISessionsPassword)
	if _, err := pool.Exec(ctx, "SELECT id, principal_id, revoked_at, legacy_token_version FROM auth.sessions WHERE id = gen_random_uuid()"); err != nil {
		t.Fatalf("the declared session columns must read: %v", err)
	}
	for _, statement := range []string{
		"SELECT token_hash FROM auth.sessions",
		"SELECT * FROM auth.sessions",
		"SELECT id FROM auth.principals",
		"UPDATE auth.sessions SET revoked_at = now()",
	} {
		_, err := pool.Exec(ctx, statement)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: %v, want SQLSTATE 42501", statement, err)
		}
	}
}

// While auth-service owns the platform roles the manifest is the session read
// plus the platform role read, exactly: an extra or a missing platform column
// refuses readiness, and both writers of the grant (the river leg and
// auth-migrate) end in the ready state in either order.
func TestQueryAPIPostureRefusesAnExtraOrMissingPlatformColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, true)
	on, off := QueryAPISessionReadFor("auth", true), QueryAPISessionReadFor("auth", false)
	if err := h.applyLeg(ctx, &on); err != nil {
		t.Fatalf("the migrate leg, owned: %v", err)
	}
	if err := h.check(t, ctx, &on); err != nil {
		t.Fatalf("exactly the manifest plus the session and platform reads must be ready: %v", err)
	}
	pool := connectAs(t, ctx, h.uri, h.role, queryAPISessionsPassword)
	if _, err := pool.Exec(ctx, `SELECT a.principal_id, a.role_key, a.expires_at, p.id, p.kind, p.disabled_at
	FROM auth.platform_role_assignments a JOIN auth.principals p ON p.id = a.principal_id`); err != nil {
		t.Fatalf("the platform role read as the role: %v", err)
	}
	if err := h.check(t, ctx, &off); err == nil || !strings.Contains(err.Error(), "outside the manifest") {
		t.Fatalf("not owned, the platform read is outside the manifest: %v", err)
	}
	for _, c := range []struct {
		statement, want string
	}{
		{"GRANT SELECT (display_name) ON auth.principals TO " + h.role, "outside the manifest"},
		{"GRANT SELECT (granted_by) ON auth.platform_role_assignments TO " + h.role, "outside the manifest"},
		{"REVOKE SELECT (kind) ON auth.principals FROM " + h.role, "lacks"},
		{"REVOKE SELECT (expires_at) ON auth.platform_role_assignments FROM " + h.role, "lacks"},
	} {
		if _, err := h.admin.Exec(ctx, c.statement); err != nil {
			t.Fatalf("%s: %v", c.statement, err)
		}
		if err := h.check(t, ctx, &on); err == nil || !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("after %q: %v, want %q", c.statement, err, c.want)
		}
		if err := h.applyLeg(ctx, &on); err != nil {
			t.Fatalf("re-apply: %v", err)
		}
		if err := h.check(t, ctx, &on); err != nil {
			t.Fatalf("after re-apply: %v", err)
		}
	}
	for _, order := range [][]bool{{true, false}, {false, true}} {
		for _, authMigrate := range order {
			if authMigrate {
				h.applyAuthMigrateGrantOwned(t, ctx, true)
			} else if err := h.applyLeg(ctx, &on); err != nil {
				t.Fatalf("river leg: %v", err)
			}
		}
		if err := h.check(t, ctx, &on); err != nil {
			t.Fatalf("order %v (auth-migrate first = %v): %v", order, order[0], err)
		}
	}
}
