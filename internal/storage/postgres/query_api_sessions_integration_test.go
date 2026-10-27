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
			"CREATE TABLE auth.principals (id uuid PRIMARY KEY)",
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
		options.QueryAPISessionRead = &riverstore.SessionReadGrant{Schema: sessions.Schema, Table: sessions.Table, Columns: sessions.Columns}
	}
	_, err := riverstore.ApplyPinnedMigrations(ctx, h.admin, options)
	return err
}

// auth-migrate's session-reader grant, built by the production builder and
// applied as the other writer of the same grant.
func (h *queryAPISessionsHarness) applyAuthMigrateGrant(t *testing.T, ctx context.Context) {
	t.Helper()
	statements, err := authschema.SessionReaderStatements(ctx, h.admin,
		authschema.Options{Schema: "auth", SessionReaderRoles: []string{h.role}})
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
	read := QueryAPISessionReadFor("auth")
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
	read := QueryAPISessionReadFor("auth")
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
	read := QueryAPISessionReadFor("auth")
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
	read := QueryAPISessionReadFor("auth")
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
