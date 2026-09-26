//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// applyQueryAPILeg runs the real migration with the query-api leg for role.
func applyQueryAPILeg(ctx context.Context, admin *pgxpool.Pool, domain, queue, role string) (riverstore.MigrationResult, error) {
	grants := make([]riverstore.TableGrant, 0)
	for _, table := range QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	return riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: grantSchema, DomainRole: domain, QueueRole: queue,
		QueryAPIRole: role, QueryAPIGrants: grants,
	})
}

// CHAOS-6804 D2616: query-api's "holds exactly the manifest" is defined ONCE, over
// the role's EFFECTIVE grants enumerated from every ACL-bearing catalog (relation,
// column, schema, function, type, language, large object, default ACL, database,
// tablespace, foreign data wrapper/server, parameter), with PUBLIC counted as
// granted to the role, plus its role-level settings and the name-resolution of the
// manifest's tables. These are the reviewers' scenarios (r1 and r2), each planted
// alone on a role that otherwise holds exactly the manifest, red first on the
// baseline that enumerated privilege KINDS instead.
func TestCheckQueryAPIAuthorizationRefusesEveryEffectiveGrantOutsideTheManifest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	for _, statement := range []string{
		"CREATE SCHEMA third_party",
		"CREATE TABLE third_party.secrets (id int, note text)",
		"INSERT INTO third_party.secrets VALUES (1, 'classified')",
		"CREATE SEQUENCE third_party.counter",
		"CREATE TYPE third_party.mood AS ENUM ('ok')",
		"CREATE FUNCTION third_party.plain() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"REVOKE EXECUTE ON FUNCTION third_party.plain() FROM PUBLIC",
		// An ordinary function whose ACL was materialised by a grant to ANOTHER role: the
		// default PUBLIC EXECUTE entry is then explicit in proacl, and must stay ambient.
		"CREATE FUNCTION third_party.materialised() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"CREATE ROLE qapi_other_role NOLOGIN",
		"GRANT EXECUTE ON FUNCTION third_party.materialised() TO qapi_other_role",
		// plpgsql itself is extension-owned (outside the scope), so the language case needs
		// an application-level language.
		"CREATE TRUSTED LANGUAGE qapi_lang HANDLER plpgsql_call_handler",
		"CREATE FOREIGN DATA WRAPPER qapi_fdw",
		"CREATE SERVER qapi_server FOREIGN DATA WRAPPER qapi_fdw",
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	dbName := fixture.dbName
	cases := []struct {
		name  string
		plant func(role string) []string
		undo  []string
		named string
	}{
		{"MAINTAIN on a table in another schema (r2 P1-1)", func(r string) []string {
			return []string{"GRANT MAINTAIN ON third_party.secrets TO " + r}
		}, nil, "MAINTAIN"},
		{"a SECURITY DEFINER function executable through PUBLIC (r2 P1-1)", func(string) []string {
			return []string{"CREATE FUNCTION third_party.leak() RETURNS bigint LANGUAGE sql SECURITY DEFINER AS 'SELECT count(*) FROM third_party.secrets'"}
		}, []string{"DROP FUNCTION third_party.leak()"}, "third_party.leak"},
		{"an explicit EXECUTE on an ordinary function", func(r string) []string {
			return []string{"GRANT EXECUTE ON FUNCTION third_party.plain() TO " + r}
		}, nil, "third_party.plain"},
		{"a granted large object (r2 P1-1)", func(r string) []string {
			return []string{
				"CREATE TEMP TABLE lo_holder AS SELECT lo_create(424242) AS oid",
				"GRANT SELECT ON LARGE OBJECT 424242 TO " + r,
			}
		}, []string{"SELECT lo_unlink(424242)"}, "large object"},
		{"USAGE alone on another schema (r2 P1-1)", func(r string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO " + r}
		}, nil, "third_party"},
		{"USAGE on a type in another schema", func(r string) []string {
			return []string{"GRANT USAGE ON TYPE third_party.mood TO " + r}
		}, nil, "third_party.mood"},
		{"a latent default privilege (ALTER DEFAULT PRIVILEGES)", func(r string) []string {
			return []string{"ALTER DEFAULT PRIVILEGES IN SCHEMA third_party GRANT SELECT ON TABLES TO " + r}
		}, []string{"ALTER DEFAULT PRIVILEGES IN SCHEMA third_party REVOKE SELECT ON TABLES FROM PUBLIC"}, "default"},
		{"CREATE on the database", func(r string) []string {
			return []string{fmt.Sprintf("GRANT CREATE ON DATABASE %s TO %s", dbName, r)}
		}, nil, "CREATE"},
		{"USAGE on a language", func(r string) []string {
			return []string{"GRANT USAGE ON LANGUAGE qapi_lang TO " + r}
		}, nil, "qapi_lang"},
		{"CREATE on a tablespace", func(r string) []string {
			return []string{"GRANT CREATE ON TABLESPACE pg_default TO " + r}
		}, nil, "pg_default"},
		{"USAGE on a foreign data wrapper", func(r string) []string {
			return []string{"GRANT USAGE ON FOREIGN DATA WRAPPER qapi_fdw TO " + r}
		}, nil, "qapi_fdw"},
		{"USAGE on a foreign server", func(r string) []string {
			return []string{"GRANT USAGE ON FOREIGN SERVER qapi_server TO " + r}
		}, nil, "qapi_server"},
		{"an explicit grant on a SYSTEM-schema object (never ambient: it is the role's own)", func(r string) []string {
			return []string{"GRANT SELECT ON pg_catalog.pg_extension TO " + r}
		}, nil, "pg_extension"},
		{"a WITH GRANT OPTION on a manifest table", func(r string) []string {
			return []string{"GRANT SELECT ON public.organizations TO " + r + " WITH GRANT OPTION"}
		}, nil, "GRANT OPTION"},
		{"a parameter privilege (SET)", func(r string) []string {
			return []string{"GRANT SET ON PARAMETER work_mem TO " + r}
		}, nil, "work_mem"},
		{"a role-level search_path that breaks unqualified lookups (r2 P1-2)", func(r string) []string {
			return []string{"ALTER ROLE " + r + " SET search_path = pg_catalog"}
		}, nil, "search_path"},
		{"a role-level setting of any kind", func(r string) []string {
			return []string{"ALTER ROLE " + r + " SET statement_timeout = '1s'"}
		}, nil, "statement_timeout"},
	}
	for index, test := range cases {
		role := fixture.newRole(t, ctx, fmt.Sprintf("_e%d", index))
		grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
		pool := fixture.connect(t, ctx, role)
		if err := CheckQueryAPIAuthorization(ctx, pool, role, "river"); err != nil {
			t.Fatalf("%s: the control role must be ready before the defect is planted: %v", test.name, err)
		}
		for _, statement := range test.plant(role) {
			if _, err := fixture.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: plant %q: %v", test.name, statement, err)
			}
		}
		// A fresh pool: a role-level setting applies to NEW sessions only.
		fresh := fixture.connect(t, ctx, role)
		err := CheckQueryAPIAuthorization(ctx, fresh, role, "river")
		if !errors.Is(err, ErrPostureRefused) || !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: error %v, want ErrUnavailable+ErrPostureRefused", test.name, err)
		} else if !strings.Contains(err.Error(), test.named) {
			t.Errorf("%s: the refusal must name %q: %v", test.name, test.named, err)
		}
		for _, statement := range test.undo {
			if _, err := fixture.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: undo %q: %v", test.name, statement, err)
			}
		}
	}
}

// Extension-owned objects are outside the application scope (lead D2616, reconciled):
// an explicit grant on an object an extension owns (pg_depend deptype 'e') is not
// counted, while the same grant on the application's own function is.
func TestCheckQueryAPIAuthorizationIgnoresExtensionOwnedObjectsButNotTheApplicationsOwn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	for _, statement := range []string{
		"CREATE SCHEMA ext_schema",
		"CREATE EXTENSION pgcrypto SCHEMA ext_schema",
		"CREATE EXTENSION pg_buffercache SCHEMA ext_schema",
		"CREATE EXTENSION citext SCHEMA ext_schema",
		"CREATE EXTENSION file_fdw",
		"CREATE FUNCTION ext_schema.app_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"REVOKE EXECUTE ON FUNCTION ext_schema.app_fn() FROM PUBLIC",
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	role := fixture.newRole(t, ctx, "_ext")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	// One explicit grant per extension-owned class: function, relation (a view),
	// type, language (plpgsql is itself an extension) and foreign data wrapper.
	for _, statement := range []string{
		"GRANT EXECUTE ON FUNCTION ext_schema.gen_random_bytes(integer) TO " + role,
		"GRANT SELECT ON ext_schema.pg_buffercache TO " + role,
		"GRANT USAGE ON TYPE ext_schema.citext TO " + role,
		"GRANT USAGE ON LANGUAGE plpgsql TO " + role,
		"GRANT USAGE ON FOREIGN DATA WRAPPER file_fdw TO " + role,
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	pool := fixture.connect(t, ctx, role)
	if err := CheckQueryAPIAuthorization(ctx, pool, role, "river"); err != nil {
		t.Fatalf("an explicit grant on an EXTENSION-owned function must be outside the scope: %v", err)
	}
	if _, err := fixture.admin.Exec(ctx, "GRANT EXECUTE ON FUNCTION ext_schema.app_fn() TO "+role); err != nil {
		t.Fatal(err)
	}
	err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "ext_schema.app_fn") {
		t.Fatalf("the same grant on the application's own function must be refused: %v", err)
	}
}

// r2 P1-2 at the CONNECTION level: a DSN-level search_path (no role setting) that
// stops the manifest's tables from resolving unqualified must fail readiness: the
// app's queries are unqualified (reports.go), so a Ready pod would fail every
// request with 42P01.
func TestCheckQueryAPIAuthorizationRefusesAConnectionWhoseSearchPathBreaksTheManifest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_sp")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	config, err := pgxpool.ParseConfig(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = role, apiAuthorizationPass
	config.ConnConfig.RuntimeParams["search_path"] = "pg_catalog"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var visible bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('saved_reports') IS NOT NULL").Scan(&visible); err != nil || visible {
		t.Fatalf("the plant did not break unqualified resolution (visible=%v err=%v)", visible, err)
	}
	err = CheckQueryAPIAuthorization(ctx, pool, role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "does not resolve") {
		t.Fatalf("a connection whose search_path hides the manifest passed readiness: %v", err)
	}
}

// r2 P1-3: the migrate leg must leave the role EXACTLY on the manifest even when it
// arrives holding grants elsewhere: its own explicit grants in every class are
// revoked from the same enumeration the check reads. PUBLIC's grants are not the
// role's to lose (other roles rely on them): they stay and the check names them.
func TestQueryAPIMigrateLegLeavesTheRoleExactlyOnTheManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	_ = uri
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	role := roles.domain + "_x"
	if len(role) > 60 {
		role = role[:60]
	}
	const password = "query_api_exact_password"
	dbName := ""
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		"CREATE DATABASE exact_other_db",
		"REVOKE CONNECT ON DATABASE exact_other_db FROM PUBLIC",
		"GRANT CONNECT, CREATE, TEMPORARY ON DATABASE exact_other_db TO " + role,
		"CREATE SCHEMA other_schema",
		"CREATE TABLE other_schema.t (id int, note text)",
		"CREATE SEQUENCE other_schema.s",
		"CREATE TYPE other_schema.mood AS ENUM ('ok')",
		"CREATE FUNCTION other_schema.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"REVOKE EXECUTE ON FUNCTION other_schema.f() FROM PUBLIC",
		"GRANT USAGE ON SCHEMA other_schema TO " + role,
		"GRANT SELECT, MAINTAIN ON other_schema.t TO " + role,
		"GRANT SELECT (note) ON other_schema.t TO " + role,
		"GRANT USAGE ON SEQUENCE other_schema.s TO " + role,
		"GRANT USAGE ON TYPE other_schema.mood TO " + role,
		"GRANT EXECUTE ON FUNCTION other_schema.f() TO " + role,
		"GRANT SELECT ON public.integrations TO " + role,
		"GRANT CREATE ON DATABASE " + dbName + " TO " + role,
		"GRANT SET ON PARAMETER work_mem TO " + role,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA other_schema GRANT SELECT ON TABLES TO " + role,
		"SELECT lo_create(515151)",
		"GRANT SELECT ON LARGE OBJECT 515151 TO " + role,
		"CREATE TRUSTED LANGUAGE exact_lang HANDLER plpgsql_call_handler",
		"GRANT USAGE ON LANGUAGE exact_lang TO " + role,
		"GRANT CREATE ON TABLESPACE pg_default TO " + role,
		"CREATE FOREIGN DATA WRAPPER exact_fdw",
		"CREATE SERVER exact_server FOREIGN DATA WRAPPER exact_fdw",
		"GRANT USAGE ON FOREIGN DATA WRAPPER exact_fdw TO " + role,
		"GRANT USAGE ON FOREIGN SERVER exact_server TO " + role,
		"ALTER ROLE " + role + " SET search_path = pg_catalog",
		"ALTER ROLE " + role + " IN DATABASE " + dbName + " SET statement_timeout = '5s'",
		"GRANT SELECT ON public.organizations TO " + role + " WITH GRANT OPTION",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS exact_other_db WITH (FORCE)")
		containers.DropRole(admin, role, t.Logf)
	})
	rolePool := connectAs(t, ctx, uri, role, password)
	if err := CheckQueryAPIAuthorization(ctx, rolePool, role, grantSchema); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("before the migrate leg the role holds grants outside the manifest: error %v", err)
	}
	if _, err := applyQueryAPILeg(ctx, admin, roles.domain, roles.queue, role); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the query-api leg: %v", err)
	}
	rolePool = connectAs(t, ctx, uri, role, password)
	if err := CheckQueryAPIAuthorization(ctx, rolePool, role, grantSchema); err != nil {
		t.Fatalf("after the migrate leg the role must hold exactly the manifest: %v", err)
	}
	var stillConnects bool
	if err := admin.QueryRow(ctx, "SELECT has_database_privilege($1, 'exact_other_db', 'CONNECT')", role).Scan(&stillConnects); err != nil || stillConnects {
		t.Fatalf("the leg must revoke the role's grant on another database (still=%v err=%v)", stillConnects, err)
	}
}

// Identity (lead D2616 (a)) for query-api: the AUTHENTICATED user must be the role.
// A superuser pool that runs SET SESSION AUTHORIZATION <role> rewrites BOTH
// session_user and current_user (r2 probe, r1 P1-2 of #3251) yet keeps the
// credential that can restore its identity; pg_stat_activity.usename of this
// backend does not change, so the predicate binds it.
func TestCheckQueryAPIAuthorizationRefusesASuperuserThatSetSessionAuthorizationToTheRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_sa")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	login := "qapi_sa_login_" + role[len(role)-8:]
	if _, err := fixture.admin.Exec(ctx, "CREATE ROLE "+login+" LOGIN SUPERUSER PASSWORD '"+apiAuthorizationPass+"'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(fixture.admin, login, t.Logf) })
	config, err := pgxpool.ParseConfig(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = login, apiAuthorizationPass
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET SESSION AUTHORIZATION "+role)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var sessionUser, currentUser string
	if err := pool.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != role || currentUser != role {
		t.Fatalf("the plant did not spoof both identities: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	err = CheckQueryAPIAuthorization(ctx, pool, role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "authenticated as") {
		t.Fatalf("a superuser pool that authorized itself as the role passed readiness: %v", err)
	}
}

// A session that SET ROLEs to another role holding the same manifest: SET ROLE
// needs membership (or superuser), and the identity predicate refuses both
// independently of current_user, so this is refused with the membership named.
func TestCheckQueryAPIAuthorizationRefusesASessionThatSetRoleToAnotherManifestHolder(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	expected := fixture.newRole(t, ctx, "_sr_e")
	alternate := fixture.newRole(t, ctx, "_sr_a")
	grantQueryAPIManifest(t, ctx, fixture.admin, expected, nil)
	grantQueryAPIManifest(t, ctx, fixture.admin, alternate, nil)
	if _, err := fixture.admin.Exec(ctx, "GRANT "+alternate+" TO "+expected); err != nil {
		t.Fatal(err)
	}
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, alternate), alternate, "river"); err != nil {
		t.Fatalf("the alternate holder must pass on its own: %v", err)
	}
	config, err := pgxpool.ParseConfig(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = expected, apiAuthorizationPass
	config.ConnConfig.RuntimeParams["role"] = alternate
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	err = CheckQueryAPIAuthorization(ctx, pool, expected, "river")
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a session authenticated as %s acting as %s passed readiness: %v", expected, alternate, err)
	}
}

// The role's own attributes are part of the identity predicate (lead D2616 (a)).
func TestCheckQueryAPIAuthorizationRefusesAnAttributeThatMakesTheRolePrivileged(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	for index, attribute := range []string{"CREATEDB", "CREATEROLE", "REPLICATION", "BYPASSRLS", "SUPERUSER", "NOLOGIN"} {
		role := fixture.newRole(t, ctx, fmt.Sprintf("_at%d", index))
		grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
		pool := fixture.connect(t, ctx, role)
		if err := CheckQueryAPIAuthorization(ctx, pool, role, "river"); err != nil {
			t.Fatalf("%s: the control must be ready: %v", attribute, err)
		}
		if _, err := fixture.admin.Exec(ctx, "ALTER ROLE "+role+" "+attribute); err != nil {
			t.Fatalf("%s: %v", attribute, err)
		}
		// The control pool already holds an open connection; NOLOGIN blocks only new logins.
		err := CheckQueryAPIAuthorization(ctx, pool, role, "river")
		if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "unprivileged login") {
			t.Errorf("%s: error %v, want a refusal naming the privileged attribute", attribute, err)
		}
	}
}

// PUBLIC's grants are not the role's to lose: the migrate leg removes the role's OWN
// grants and leaves PUBLIC's alone (other roles rely on them); the check then
// names the PUBLIC grant so an operator decides.
func TestQueryAPIMigrateLegLeavesPublicGrantsAndTheCheckNamesThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	role := roles.domain + "_p"
	if len(role) > 60 {
		role = role[:60]
	}
	const password = "query_api_public_password"
	var dbName string
	if err := admin.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		"CREATE SCHEMA shared_reporting",
		"CREATE TABLE shared_reporting.facts (id int)",
		"GRANT USAGE ON SCHEMA shared_reporting TO PUBLIC",
		"GRANT SELECT ON shared_reporting.facts TO PUBLIC",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := applyQueryAPILeg(ctx, admin, roles.domain, roles.queue, role); err != nil {
		t.Fatalf("the migrate leg: %v", err)
	}
	var stillPublic bool
	if err := admin.QueryRow(ctx, "SELECT has_table_privilege('public', 'shared_reporting.facts', 'SELECT')").Scan(&stillPublic); err != nil || !stillPublic {
		t.Fatalf("the leg must not touch PUBLIC's grant (still=%v err=%v)", stillPublic, err)
	}
	err := CheckQueryAPIAuthorization(ctx, connectAs(t, ctx, uri, role, password), role, grantSchema)
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "shared_reporting") || !strings.Contains(err.Error(), "PUBLIC") {
		t.Fatalf("a PUBLIC grant must be named by the check: %v", err)
	}
}

func TestDiffQueryAPIGrantsNamesTheFirstDifferenceDeterministically(t *testing.T) {
	t.Parallel()
	posture := RolePosture{RequiredTables: []TablePrivilege{{"a_table", true, false, false}, {"b_table", false, false, false}}}
	full := []roleacl.Grant{
		{Class: "database", Object: "db", Privilege: "CONNECT"},
		{Class: "database", Object: "db", Privilege: "CONNECT", ViaPublic: true},
		{Class: "schema", Object: "public", Privilege: "USAGE"},
		{Class: "relation", Object: "public.a_table", Privilege: "SELECT"},
		{Class: "relation", Object: "public.a_table", Privilege: "INSERT"},
		{Class: "relation", Object: "public.b_table", Privilege: "SELECT"},
	}
	if got := diffQueryAPIGrants("db", posture, full); got != "" {
		t.Fatalf("the exact set must have no difference: %q", got)
	}
	extra := append(append([]roleacl.Grant(nil), full...), roleacl.Grant{Class: "relation", Object: "public.c_table", Privilege: "MAINTAIN"})
	if got := diffQueryAPIGrants("db", posture, extra); !strings.Contains(got, "MAINTAIN") || !strings.Contains(got, "outside the manifest") {
		t.Fatalf("an extra grant must be named: %q", got)
	}
	withGrantOption := append(append([]roleacl.Grant(nil), full...), roleacl.Grant{Class: "relation", Object: "public.a_table", Privilege: "SELECT WITH GRANT OPTION"})
	if got := diffQueryAPIGrants("db", posture, withGrantOption); !strings.Contains(got, "GRANT OPTION") {
		t.Fatalf("a grant option is a different privilege: %q", got)
	}
	missing := full[:len(full)-1]
	if got := diffQueryAPIGrants("db", posture, missing); got != "lacks SELECT on relation public.b_table" {
		t.Fatalf("a missing manifest privilege must be named: %q", got)
	}
	noUsage := append([]roleacl.Grant(nil), full[:2]...)
	noUsage = append(noUsage, full[3:]...)
	if got := diffQueryAPIGrants("db", posture, noUsage); got != "lacks USAGE on schema public" {
		t.Fatalf("a missing USAGE on public must be named: %q", got)
	}
}

// CHAOS-6804 r3b P1-1: ownership of ANY object class, not only the four the
// preflight listed. Ownership carries every privilege on the object (including
// DROP) and leaves no ACL entry, so the enumeration cannot see it: pg_shdepend
// (deptype 'o') records the ownership of EVERY object class, and the check reads it.
func TestCheckQueryAPIAuthorizationRefusesOwnershipOfAnyObjectClass(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	cases := []struct {
		name          string
		create, owner string
	}{
		{"a collation", "CREATE COLLATION public.qapi_owned_collation (provider = libc, locale = 'C')", "ALTER COLLATION public.qapi_owned_collation OWNER TO %s"},
		{"a text search configuration", "CREATE TEXT SEARCH CONFIGURATION public.qapi_owned_tsconfig (COPY = simple)", "ALTER TEXT SEARCH CONFIGURATION public.qapi_owned_tsconfig OWNER TO %s"},
		{"a text search dictionary", "CREATE TEXT SEARCH DICTIONARY public.qapi_owned_tsdict (TEMPLATE = simple)", "ALTER TEXT SEARCH DICTIONARY public.qapi_owned_tsdict OWNER TO %s"},
		{"an enum type", "CREATE TYPE public.qapi_owned_enum AS ENUM ('a')", "ALTER TYPE public.qapi_owned_enum OWNER TO %s"},
		{"a domain", "CREATE DOMAIN public.qapi_owned_domain AS int", "ALTER DOMAIN public.qapi_owned_domain OWNER TO %s"},
		{"a conversion", "CREATE CONVERSION public.qapi_owned_conversion FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1", "ALTER CONVERSION public.qapi_owned_conversion OWNER TO %s"},
		{"an operator", "CREATE FUNCTION public.qapi_op_fn(int, int) RETURNS bool LANGUAGE sql AS 'SELECT true'; CREATE OPERATOR public.=== (LEFTARG = int, RIGHTARG = int, FUNCTION = public.qapi_op_fn)", "ALTER OPERATOR public.===(int, int) OWNER TO %s"},
	}
	for index, test := range cases {
		role := fixture.newRole(t, ctx, fmt.Sprintf("_own%d", index))
		grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
		if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
			t.Fatalf("%s: the control must be ready: %v", test.name, err)
		}
		if _, err := fixture.admin.Exec(ctx, test.create); err != nil {
			t.Fatalf("%s: create: %v", test.name, err)
		}
		if _, err := fixture.admin.Exec(ctx, fmt.Sprintf(test.owner, role)); err != nil {
			t.Fatalf("%s: transfer: %v", test.name, err)
		}
		err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
		if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "owns") {
			t.Errorf("%s: a role that owns it passed readiness: %v", test.name, err)
		}
	}
}

// CHAOS-6804 r3b P1-2: an explicit grant TO THE ROLE on ANOTHER database. The
// catalogs of grants are per database but pg_database is shared, so the role's own
// grants on every database are enumerable: the baseline is CONNECT on THIS database
// only. (PUBLIC's default CONNECT/TEMPORARY on other databases is ambient.)
func TestCheckQueryAPIAuthorizationRefusesAnExplicitGrantOnAnotherDatabase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_odb")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	other := "qapi_other_" + role[len(role)-8:]
	for _, statement := range []string{
		"CREATE DATABASE " + other,
		"REVOKE CONNECT ON DATABASE " + other + " FROM PUBLIC",
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = fixture.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+other+" WITH (FORCE)")
	})
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
		t.Fatalf("the control must be ready: %v", err)
	}
	for _, privilege := range []string{"CONNECT", "CREATE", "TEMPORARY"} {
		if _, err := fixture.admin.Exec(ctx, "GRANT "+privilege+" ON DATABASE "+other+" TO "+role); err != nil {
			t.Fatal(err)
		}
		err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
		if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), other) {
			t.Errorf("%s on another database passed readiness: %v", privilege, err)
		}
		if _, err := fixture.admin.Exec(ctx, "REVOKE "+privilege+" ON DATABASE "+other+" FROM "+role); err != nil {
			t.Fatal(err)
		}
	}
}

// A grant on an object INSIDE another database cannot be enumerated from this
// database's catalogs, but pg_shdepend records it (dbid = that database): the
// self-check reports it as an unexplained ACL dependency and readiness refuses.
func TestCheckQueryAPIAuthorizationRefusesAGrantOnAnObjectInAnotherDatabase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_odbobj")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	other := "qapi_objdb_" + role[len(role)-8:]
	if _, err := fixture.admin.Exec(ctx, "CREATE DATABASE "+other); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fixture.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+other+" WITH (FORCE)")
	})
	parsed, err := url.Parse(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + other
	otherAdmin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherAdmin.Close)
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
		t.Fatalf("the control must be ready: %v", err)
	}
	for _, statement := range []string{"CREATE TABLE other_facts (id int)", "GRANT SELECT ON other_facts TO " + role} {
		if _, err := otherAdmin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	err = CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "unexplained ACL dependency") {
		t.Fatalf("a grant on an object in another database passed readiness: %v", err)
	}
}

// r4 P1, resolved as a documented scope rule: the extension-owned exclusion applies to
// objects in THIS database only (pg_depend is per-database). An ACL entry for the role
// on an extension-owned object in ANOTHER database cannot be classified from here, so
// it is refused, named, and stops the migrate leg: fail closed, and outside the
// manifest either way.
func TestCheckQueryAPIAuthorizationRefusesAnExtensionObjectGrantInAnotherDatabaseByDesign(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_extodb")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	other := "qapi_extdb_" + role[len(role)-8:]
	if _, err := fixture.admin.Exec(ctx, "CREATE DATABASE "+other); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fixture.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+other+" WITH (FORCE)")
	})
	parsed, err := url.Parse(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + other
	otherAdmin, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherAdmin.Close)
	if _, err := otherAdmin.Exec(ctx, "GRANT EXECUTE ON FUNCTION pg_catalog.plpgsql_call_handler() TO "+role); err != nil {
		t.Fatal(err)
	}
	err = CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "unexplained ACL dependency") {
		t.Fatalf("an extension-owned object's grant in another database must be refused and named: %v", err)
	}
}
