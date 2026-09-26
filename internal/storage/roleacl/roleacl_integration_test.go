//go:build integration

package roleacl

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6804 (lead D2623): the enumeration is closed by a SELF-CHECK. pg_shdepend
// records an 'a' row for every ACL entry granted to the role, in every class and
// database; each must be explained by an enumerated grant (or be extension-owned).
// These tests hold a role that has an ACL entry in EVERY class, then prove that (1)
// nothing is unexplained, and (2) disabling ANY one branch of the enumeration makes
// the self-check report the gap, so an enumeration that misses a class cannot pass
// silently: it is refused (check) or stops the migration (leg).
func TestSelfCheckExplainsEveryACLDependencyAndCatchesAMissingBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var db string
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
		t.Fatal(err)
	}
	const role = "selfcheck_role"
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER PASSWORD 'x'",
		"CREATE SCHEMA app",
		"CREATE TABLE app.t (id int, note text)",
		"CREATE SEQUENCE app.s",
		"CREATE TYPE app.mood AS ENUM ('ok')",
		"CREATE FUNCTION app.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"CREATE TRUSTED LANGUAGE app_lang HANDLER plpgsql_call_handler",
		"CREATE FOREIGN DATA WRAPPER app_fdw",
		"CREATE SERVER app_server FOREIGN DATA WRAPPER app_fdw",
		"SELECT lo_create(616161)",
		"GRANT USAGE ON SCHEMA app TO " + role,
		"GRANT SELECT ON app.t TO " + role,
		"GRANT SELECT (note) ON app.t TO " + role,
		"GRANT USAGE ON SEQUENCE app.s TO " + role,
		"GRANT USAGE ON TYPE app.mood TO " + role,
		"GRANT EXECUTE ON FUNCTION app.f() TO " + role,
		"GRANT USAGE ON LANGUAGE app_lang TO " + role,
		"GRANT USAGE ON FOREIGN DATA WRAPPER app_fdw TO " + role,
		"GRANT USAGE ON FOREIGN SERVER app_server TO " + role,
		"GRANT SELECT ON LARGE OBJECT 616161 TO " + role,
		"GRANT CONNECT ON DATABASE " + db + " TO " + role,
		"GRANT CREATE ON TABLESPACE pg_default TO " + role,
		"GRANT SET ON PARAMETER work_mem TO " + role,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	unexplained := func(disabled map[string]bool) []Grant {
		grants, err := enumerate(ctx, admin, role, disabled)
		if err != nil {
			t.Fatal(err)
		}
		var out []Grant
		for _, grant := range grants {
			if grant.Class == UnexplainedClass {
				out = append(out, grant)
			}
		}
		return out
	}
	if got := unexplained(nil); len(got) != 0 {
		t.Fatalf("with every branch on, the role's ACL entries must all be explained, got %v", got)
	}
	// Each class whose ACL entries pg_shdepend tracks: turning its branch off must
	// surface as unexplained. (The role-setting branch is not an ACL class.)
	for _, class := range []string{
		"relation", "column", "schema", "function", "type", "language", "large object",
		"default privileges", "database", "tablespace", "foreign data wrapper", "foreign server", "parameter",
	} {
		if got := unexplained(map[string]bool{class: true}); len(got) == 0 {
			t.Errorf("disabling the %q branch left every ACL dependency explained: the self-check does not notice that class missing", class)
		}
	}
	// RevokeStatements refuses to provision a role it cannot fully explain.
	revokes, err := RevokeStatements(ctx, admin, role)
	if err != nil || len(revokes) == 0 {
		t.Fatalf("a fully explained role must yield revoke statements (n=%d err=%v)", len(revokes), err)
	}
}

// A role holding an ACL entry the enumeration cannot see (an object in ANOTHER
// database) must stop the migration leg rather than be provisioned as if exact: the
// leg's sweep is derived from the same enumeration, so it cannot revoke what it
// cannot see.
func TestRevokeStatementsRefuseARoleWithAnUnexplainedACLDependency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	const role = "unexplained_role"
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER PASSWORD 'x'",
		"CREATE DATABASE unexplained_other_db",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS unexplained_other_db WITH (FORCE)")
		containers.DropRole(admin, role, t.Logf)
	})
	if statements, err := RevokeStatements(ctx, admin, role); err != nil {
		t.Fatalf("a role with nothing unexplained must yield statements (%v, err %v)", statements, err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/unexplained_other_db"
	other, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	for _, statement := range []string{"CREATE TABLE facts (id int)", "GRANT SELECT ON facts TO " + role} {
		if _, err := other.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if statements, err := RevokeStatements(ctx, admin, role); err == nil {
		t.Fatalf("a role with an ACL entry in another database must stop the leg, got statements %v", statements)
	}
}
