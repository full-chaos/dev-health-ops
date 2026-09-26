//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const queryAPIAuthorizationRole = "devhealth_query_api_authorization_test"

// queryAPIFixture starts one Postgres with stand-ins for every table the
// query-api manifest names plus an unrelated one. Each test then makes as many
// plain login roles as it needs with newRole: USAGE on public and nothing else,
// so every grant a test adds is explicit.
type queryAPIFixture struct {
	admin  *pgxpool.Pool
	uri    string
	dbName string
	inst   *containers.Instance
}

func startQueryAPIFixture(t *testing.T) *queryAPIFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePostgresInstanceInternal(t, instance) })
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS unrelated_read_table (id uuid PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	return &queryAPIFixture{admin: admin, uri: instance.URI, dbName: dbName, inst: instance}
}

func (f *queryAPIFixture) newRole(t *testing.T, ctx context.Context, suffix string) string {
	t.Helper()
	role, err := containers.RoleName(queryAPIAuthorizationRole+suffix, f.inst)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(f.admin, role, t.Logf) })
	bootstrapAPIRole(t, ctx, f.admin, f.dbName, role)
	return role
}

func (f *queryAPIFixture) connect(t *testing.T, ctx context.Context, role string) *pgxpool.Pool {
	t.Helper()
	return connectAs(t, ctx, f.uri, role, apiAuthorizationPass)
}

func grantQueryAPIManifest(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string, skip func(TablePrivilege, string) bool) {
	t.Helper()
	for _, table := range QueryAPIPosture().RequiredTables {
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
			wanted := privilege == "SELECT" ||
				(privilege == "INSERT" && table.AllowInsert) ||
				(privilege == "UPDATE" && table.AllowUpdate) ||
				(privilege == "DELETE" && table.AllowDelete)
			if !wanted || (skip != nil && skip(table, privilege)) {
				continue
			}
			if _, err := admin.Exec(ctx, "GRANT "+privilege+" ON "+table.TableName+" TO "+role); err != nil {
				t.Fatalf("grant %s on %s: %v", privilege, table.TableName, err)
			}
		}
	}
}

// The role holds exactly the declared manifest: ready.
func TestCheckQueryAPIAuthorizationAcceptsExactlyTheManifest(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_ok")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
		t.Fatalf("a role holding exactly the manifest failed readiness: %v", err)
	}
}

// CHAOS-6804: this is a HOLD-EXACTLY posture. Every way a role can hold more
// than the manifest is refused: the read plane it lives on today (a SELECT on
// an unlisted table), a write on a read-only relation, an undeclared privilege
// on a declared relation, a PUBLIC grant, and ownership of any object. Each is
// planted alone on a role that otherwise holds the whole manifest, so the
// refusal can only come from the planted defect.
func TestCheckQueryAPIAuthorizationRefusesEveryWayOfHoldingMore(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	cases := []struct {
		name      string
		plant     func(role string) []string
		wantNamed string
	}{
		{"select on an unlisted table", func(r string) []string {
			return []string{"GRANT SELECT ON unrelated_read_table TO " + r}
		}, "unrelated_read_table"},
		{"insert on a read-only relation", func(r string) []string {
			return []string{"GRANT INSERT ON go_api_routing_state TO " + r}
		}, "go_api_routing_state"},
		{"update on a read-only relation", func(r string) []string {
			return []string{"GRANT UPDATE ON organizations TO " + r}
		}, "organizations"},
		{"delete beyond saved_reports", func(r string) []string {
			return []string{"GRANT DELETE ON scheduled_jobs TO " + r}
		}, "scheduled_jobs"},
		{"truncate on a declared relation", func(r string) []string {
			return []string{"GRANT TRUNCATE ON saved_reports TO " + r}
		}, "saved_reports"},
		{"a PUBLIC grant on an unlisted table", func(string) []string {
			return []string{"GRANT SELECT ON unrelated_read_table TO PUBLIC"}
		}, "unrelated_read_table"},
		{"ownership of a table", func(r string) []string {
			return []string{"CREATE TABLE owned_by_query_api (id int)", "ALTER TABLE owned_by_query_api OWNER TO " + r}
		}, ""},
		{"a grant through role membership", func(r string) []string {
			return []string{
				"CREATE ROLE qapi_member_source NOLOGIN",
				"GRANT SELECT ON unrelated_read_table TO qapi_member_source",
				"GRANT qapi_member_source TO " + r,
			}
		}, ""},
	}
	for index, test := range cases {
		role := fixture.newRole(t, ctx, fmt.Sprintf("_x%d", index))
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
		err := CheckQueryAPIAuthorization(ctx, pool, role, "river")
		if !errors.Is(err, ErrPostureRefused) || !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: error %v, want ErrUnavailable+ErrPostureRefused", test.name, err)
			continue
		}
		if test.wantNamed != "" && !strings.Contains(err.Error(), test.wantNamed) {
			t.Errorf("%s: the refusal must name %s: %v", test.name, test.wantNamed, err)
		}
		// Undo so the shared PUBLIC / membership plants do not leak into the next case.
		for _, statement := range []string{
			"REVOKE SELECT ON unrelated_read_table FROM PUBLIC",
			"DROP TABLE IF EXISTS owned_by_query_api",
			"DROP ROLE IF EXISTS qapi_member_source",
		} {
			if _, err := fixture.admin.Exec(ctx, statement); err != nil && !strings.Contains(err.Error(), "cannot be dropped") {
				t.Fatalf("%s: undo %q: %v", test.name, statement, err)
			}
		}
	}
}

// Each declared privilege, withheld alone, refuses readiness and names the
// table: the check is per (table, privilege), not per table.
func TestCheckQueryAPIAuthorizationRefusesEveryMissingDeclaredPrivilege(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	cases := 0
	for _, table := range QueryAPIPosture().RequiredTables {
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
			declared := privilege == "SELECT" ||
				(privilege == "INSERT" && table.AllowInsert) ||
				(privilege == "UPDATE" && table.AllowUpdate) ||
				(privilege == "DELETE" && table.AllowDelete)
			if !declared {
				continue
			}
			cases++
			role := fixture.newRole(t, ctx, fmt.Sprintf("_m%d", cases))
			grantQueryAPIManifest(t, ctx, fixture.admin, role, func(tp TablePrivilege, p string) bool {
				return tp.TableName == table.TableName && p == privilege
			})
			err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
			if !errors.Is(err, ErrPostureRefused) || !errors.Is(err, ErrUnavailable) {
				t.Errorf("missing %s on %s: error %v, want ErrUnavailable+ErrPostureRefused", privilege, table.TableName, err)
				continue
			}
			if !strings.Contains(err.Error(), table.TableName) {
				t.Errorf("the refusal for missing %s on %s must name the table: %v", privilege, table.TableName, err)
			}
		}
	}
	// 13 tables: SELECT on each, plus 3+2+1+1 write flags.
	if cases != 13+7 {
		t.Fatalf("exercised %d (table, privilege) cases, want 20: the manifest changed without this test", cases)
	}
}

// Grants on a role the pool does not log in as are not the pool's grants.
func TestCheckQueryAPIAuthorizationRefusesALoginThatIsNotTheNamedRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_login")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	// The admin pool logs in as the container's superuser, not the named role.
	err := CheckQueryAPIAuthorization(ctx, fixture.admin, role, "river")
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a pool logged in as another role: error %v, want ErrPostureRefused", err)
	}
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
		t.Fatalf("the same grants over the named login: %v", err)
	}
	// A name that is not a valid runtime identifier is refused as unusable
	// configuration, before the login is even compared: it is not a posture
	// refusal of a role that exists.
	err = CheckQueryAPIAuthorization(ctx, fixture.admin, "Not A Role; DROP TABLE x", "river")
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPostureRefused) {
		t.Fatalf("an invalid role name: error %v, want ErrUnavailable without ErrPostureRefused", err)
	}
}

// A declared table that does not exist is a gap, not a pass.
func TestCheckQueryAPIAuthorizationRefusesAMissingTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_notable")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	if _, err := fixture.admin.Exec(ctx, "DROP TABLE report_runs"); err != nil {
		t.Fatal(err)
	}
	err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river")
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "report_runs") {
		t.Fatalf("a missing declared table: error %v", err)
	}
}

// The whole chain on production-shaped tables: the real migrate leg turns a
// role that reads MORE than the manifest (what the registry-owner-style DSN
// does today) into exactly the manifest, readiness turns ready, the statements
// the saved-report mutations issue succeed as that role, and everything the
// manifest does not name is denied.
func TestQueryAPIRoleEndToEndThroughTheMigrateLeg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	role := roles.domain + "_qapi"
	if len(role) > 60 {
		role = role[:60]
	}
	const password = "query_api_e2e_password"
	dbName, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		// What the role holds today when it logs in with a wide DSN: reads of
		// tables the manifest never names, and a privilege it never uses. The
		// migrate leg must REMOVE both.
		"GRANT SELECT ON public.integrations TO " + role,
		"GRANT TRUNCATE ON public.sync_runs TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	queryAPI := connectAs(t, ctx, uri, role, password)

	if err := CheckQueryAPIAuthorization(ctx, queryAPI, role, grantSchema); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("before the migrate leg the role holds the wrong grants: error %v", err)
	}

	grants := make([]riverstore.TableGrant, 0)
	for _, table := range QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: grantSchema, DomainRole: roles.domain, QueueRole: roles.queue,
		QueryAPIRole: role, QueryAPIGrants: grants,
	}); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the query-api leg: %v", err)
	}
	if err := CheckQueryAPIAuthorization(ctx, queryAPI, role, grantSchema); err != nil {
		t.Fatalf("after the migrate leg the role must be ready: %v", err)
	}

	// The leg REVOKEs: what the role held beyond the manifest is gone.
	for _, check := range []struct{ table, privilege string }{
		{"public.integrations", "SELECT"}, {"public.sync_runs", "TRUNCATE"},
	} {
		var held bool
		if err := admin.QueryRow(ctx, "SELECT has_table_privilege($1, $2, $3)", role, check.table, check.privilege).Scan(&held); err != nil || held {
			t.Errorf("the migrate leg left %s on %s with the query-api role (held=%v, err=%v)", check.privilege, check.table, held, err)
		}
	}

	// Allowed: the statement shapes the mutations issue, in one transaction as
	// triggerReport does (run + outbox together).
	allowed := []string{
		"INSERT INTO public.saved_reports (id, org_id, name) VALUES ('11111111-1111-4111-8111-111111111111', 'o', 'n')",
		"INSERT INTO public.scheduled_jobs (id, org_id, job_type, schedule_cron, created_at, updated_at) VALUES ('22222222-2222-4222-8222-222222222222', 'o', 'report', '* * * * *', now(), now())",
		"UPDATE public.saved_reports SET name = 'x', updated_at = now() WHERE org_id = 'o'",
		"UPDATE public.scheduled_jobs SET schedule_cron = '0 * * * *', next_run_at = now() WHERE org_id = 'o'",
		"INSERT INTO public.report_runs (id, report_id, triggered_by) VALUES ('33333333-3333-4333-8333-333333333333', '11111111-1111-4111-8111-111111111111', 'api')",
		`INSERT INTO public.worker_job_outbox (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at, status, attempt_count, next_attempt_at, created_at, updated_at)
			VALUES ('44444444-4444-4444-8444-444444444444', 'report.run:3', 'report.execute_on_demand', 1, '{}', 'sha256:x', 'reports', 0, 3, now(), 'pending', 0, now(), now(), now())
			ON CONFLICT (dedupe_key) DO NOTHING`,
		"SELECT job_kind, status FROM public.worker_job_outbox WHERE dedupe_key = 'report.run:3'",
		"SELECT id FROM public.go_api_routing_state LIMIT 1",
		"SELECT id FROM public.settings LIMIT 1",
		"DELETE FROM public.saved_reports WHERE org_id = 'o'",
	}
	tx, err := queryAPI.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range allowed {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Errorf("denied to the query-api role: %v\n  statement: %s", err, strings.Join(strings.Fields(statement), " "))
		}
	}
	_ = tx.Rollback(ctx)

	// Denied: everything the manifest does not name. 42501 is the permission
	// error; any other failure would mean the statement never reached the ACL.
	denied := []string{
		"DELETE FROM public.scheduled_jobs WHERE org_id = 'o'",
		"UPDATE public.report_runs SET status = 'x' WHERE id = '33333333-3333-4333-8333-333333333333'",
		"DELETE FROM public.report_runs WHERE id = '33333333-3333-4333-8333-333333333333'",
		"UPDATE public.worker_job_outbox SET status = 'x' WHERE dedupe_key = 'report.run:3'",
		"DELETE FROM public.worker_job_outbox WHERE dedupe_key = 'report.run:3'",
		"INSERT INTO public.go_api_routing_state (id) VALUES ('66666666-6666-4666-8666-666666666666')",
		"TRUNCATE public.saved_reports",
		"SELECT id FROM public.integrations",
		"INSERT INTO public.integrations (id) VALUES ('55555555-5555-4555-8555-555555555555')",
	}
	for _, statement := range denied {
		tx, err := queryAPI.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, execErr := tx.Exec(ctx, statement)
		_ = tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if !errors.As(execErr, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("expected permission denied (42501), got %v\n  statement: %s", execErr, statement)
		}
	}
}

// CHAOS-6804: the leg REVOKEs, so pointing it at a role that must not be
// revoked stops the migration BEFORE any statement runs. The two shapes that
// matter in production: the role that OWNS objects (the registry owner
// query-api logs in as today) and the migration identity itself.
func TestQueryAPILegRefusesARoleItMustNeverRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	dbName, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range QueryAPIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}
	grants := make([]riverstore.TableGrant, 0)
	for _, table := range QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	apply := func(role string) error {
		_, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
			Schema: grantSchema, DomainRole: roles.domain, QueueRole: roles.queue,
			QueryAPIRole: role, QueryAPIGrants: grants,
		})
		return err
	}

	// Owners: least-privilege-looking logins that own SOMETHING. Each clause of
	// the leg's "owns nothing" predicate is exercised alone (a wholesale
	// mutation of the compound predicate could hide a wrong clause).
	suffixes := []struct {
		name  string
		plant func(role string) []string
		undo  func(role string) []string
	}{
		{"table", func(r string) []string {
			return []string{"ALTER TABLE public.go_api_routing_state OWNER TO " + r}
		}, func(string) []string {
			return []string{"ALTER TABLE public.go_api_routing_state OWNER TO CURRENT_USER"}
		}},
		{"schema", func(r string) []string {
			return []string{"CREATE SCHEMA qapi_owned_schema AUTHORIZATION " + r}
		}, func(string) []string { return []string{"DROP SCHEMA IF EXISTS qapi_owned_schema CASCADE"} }},
		{"function", func(r string) []string {
			return []string{
				"CREATE FUNCTION public.qapi_owned_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'",
				"ALTER FUNCTION public.qapi_owned_fn() OWNER TO " + r,
			}
		}, func(string) []string { return []string{"DROP FUNCTION IF EXISTS public.qapi_owned_fn()"} }},
		{"database", func(r string) []string {
			return []string{"CREATE DATABASE qapi_owned_db OWNER " + r}
		}, func(string) []string { return []string{"DROP DATABASE IF EXISTS qapi_owned_db"} }},
		// r3b P1-1: classes with no ACL entry the enumeration could see.
		{"collation", func(r string) []string {
			return []string{
				"CREATE COLLATION public.qapi_owned_coll (provider = libc, locale = 'C')",
				"ALTER COLLATION public.qapi_owned_coll OWNER TO " + r,
			}
		}, func(string) []string { return []string{"DROP COLLATION IF EXISTS public.qapi_owned_coll"} }},
		{"enum type", func(r string) []string {
			return []string{"CREATE TYPE public.qapi_owned_enum AS ENUM ('a')", "ALTER TYPE public.qapi_owned_enum OWNER TO " + r}
		}, func(string) []string { return []string{"DROP TYPE IF EXISTS public.qapi_owned_enum"} }},
		{"text search configuration", func(r string) []string {
			return []string{
				"CREATE TEXT SEARCH CONFIGURATION public.qapi_owned_ts (COPY = simple)",
				"ALTER TEXT SEARCH CONFIGURATION public.qapi_owned_ts OWNER TO " + r,
			}
		}, func(string) []string {
			return []string{"DROP TEXT SEARCH CONFIGURATION IF EXISTS public.qapi_owned_ts"}
		}},
	}
	for index, owned := range suffixes {
		owner := fmt.Sprintf("%s_o%d", roles.domain, index)
		if len(owner) > 60 {
			owner = owner[:60]
		}
		for _, statement := range append([]string{
			"CREATE ROLE " + owner + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
			"GRANT CONNECT ON DATABASE " + dbName + " TO " + owner,
		}, owned.plant(owner)...) {
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s owner: %s: %v", owned.name, statement, err)
			}
		}
		err := apply(owner)
		for _, statement := range owned.undo(owner) {
			_, _ = admin.Exec(context.Background(), statement)
		}
		_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+owner)
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+owner)
		if !errors.Is(err, riverstore.ErrMigrationConfiguration) {
			t.Errorf("naming a role that owns a %s: error %v, want ErrMigrationConfiguration", owned.name, err)
		}
	}
	// The refused legs never revoked anything from the (former) table owner's
	// objects: the table is still readable by the migration identity and its
	// ACL is untouched.
	var aclUntouched bool
	if err := admin.QueryRow(ctx, "SELECT relacl IS NULL FROM pg_class WHERE oid = 'public.go_api_routing_state'::regclass").Scan(&aclUntouched); err != nil || !aclUntouched {
		t.Fatalf("a refused leg changed the table ACL (untouched=%v, err=%v)", aclUntouched, err)
	}

	// Ineligible logins, one attribute at a time: each clause of the leg's
	// eligibility predicate must refuse alone.
	for index, attribute := range []string{"SUPERUSER", "CREATEDB", "CREATEROLE", "REPLICATION", "BYPASSRLS"} {
		role := fmt.Sprintf("%s_a%d", roles.domain, index)
		if len(role) > 60 {
			role = role[:60]
		}
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN "+attribute+" PASSWORD 'x'"); err != nil {
			t.Fatalf("%s role: %v", attribute, err)
		}
		err := apply(role)
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		if !errors.Is(err, riverstore.ErrMigrationConfiguration) {
			t.Errorf("naming a %s role: error %v, want ErrMigrationConfiguration", attribute, err)
		}
	}
	noLogin := roles.domain + "_nl"
	if len(noLogin) > 60 {
		noLogin = noLogin[:60]
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE "+noLogin+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	err = apply(noLogin)
	_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+noLogin)
	if !errors.Is(err, riverstore.ErrMigrationConfiguration) {
		t.Errorf("naming a NOLOGIN role: error %v, want ErrMigrationConfiguration", err)
	}

	// The migration identity (a superuser here, as on bigboy).
	var migrationRole string
	if err := admin.QueryRow(ctx, "SELECT current_user").Scan(&migrationRole); err != nil {
		t.Fatal(err)
	}
	if err := apply(migrationRole); !errors.Is(err, riverstore.ErrMigrationConfiguration) {
		t.Fatalf("naming the migration identity: error %v, want ErrMigrationConfiguration", err)
	}
}

// CHAOS-6804: the REAL scripts/worker/provision_river_roles.sql through psql
// with query_api_role set (not a Go re-implementation of it). The role it
// produces is the bare baseline (login, CONNECT, USAGE on public, nothing
// else), so it correctly FAILS the posture check until the migrate leg grants
// the manifest; granting the manifest is what gets it to ready. A default run
// (query_api_role unset) creates no such role, and a name that collides with
// another role's is refused with a non-zero exit.
func TestProvisionScriptQueryAPIRoleOptIn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role, err := containers.RoleName("provision_qapi_opt_in", fixture.inst)
	if err != nil {
		t.Fatal(err)
	}
	unused := []string{"provision_qapi_domain_unused", "provision_qapi_queue_unused", "provision_qapi_coordinator_unused"}
	t.Cleanup(func() {
		for _, name := range append([]string{role}, unused...) {
			containers.DropRole(fixture.admin, name, t.Logf)
		}
	})
	if _, err := fixture.admin.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS river"); err != nil {
		t.Fatal(err)
	}
	run := func(extra ...string) ([]byte, error) {
		args := []string{
			fixture.uri,
			"--set=ON_ERROR_STOP=1",
			"--set=domain_role=" + unused[0], "--set=queue_role=" + unused[1], "--set=coordinator_role=" + unused[2],
			"--set=domain_password=unused", "--set=queue_password=unused", "--set=coordinator_password=unused",
			"--file=" + provisionScriptPath(t),
		}
		return exec.CommandContext(ctx, "psql", append(args, extra...)...).CombinedOutput()
	}
	roleExists := func(name string) bool {
		var exists bool
		if err := fixture.admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", name).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		return exists
	}

	// Default run: no query-api role.
	if output, err := run(); err != nil {
		t.Fatalf("default provisioning run failed: %v\n%s", err, output)
	}
	if roleExists(role) {
		t.Fatal("a run without query_api_role created the query-api role")
	}

	// Collisions with another role's name are refused by THIS block (the message
	// names the colliding role). The api and keda blocks run first and may have
	// created their own role; that is theirs, not the query-api role.
	for name, refusal := range map[string]struct {
		extra []string
		want  string
	}{
		"the domain role": {[]string{"--set=query_api_role=" + unused[0], "--set=query_api_password=x"}, "query_api_role must be distinct from domain_role"},
		"the api role": {[]string{"--set=api_role=provision_qapi_api_x", "--set=api_password=x",
			"--set=query_api_role=provision_qapi_api_x", "--set=query_api_password=x"}, "query_api_role must be distinct from api_role"},
		"the keda role": {[]string{"--set=keda_role=provision_qapi_keda_x", "--set=keda_password=x",
			"--set=query_api_role=provision_qapi_keda_x", "--set=query_api_password=x"}, "query_api_role must be distinct from keda_role"},
	} {
		output, err := run(refusal.extra...)
		if err == nil || !strings.Contains(string(output), refusal.want) {
			t.Errorf("query_api_role equal to %s must fail the script with %q (err %v):\n%s", name, refusal.want, err, output)
		}
	}
	t.Cleanup(func() {
		for _, leaked := range []string{"provision_qapi_api_x", "provision_qapi_keda_x"} {
			containers.DropRole(fixture.admin, leaked, t.Logf)
		}
	})

	// Opt-in run.
	if output, err := run("--set=query_api_role="+role, "--set=query_api_password="+apiAuthorizationPass); err != nil {
		t.Fatalf("psql --file=provision_river_roles.sql (query_api_role opt-in) failed: %v\n%s", err, output)
	}
	queryAPI := fixture.connect(t, ctx, role)
	err = CheckQueryAPIAuthorization(ctx, queryAPI, role, "river")
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("the bare script-provisioned role must be refused until the manifest is granted: error %v", err)
	}
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	if err := CheckQueryAPIAuthorization(ctx, queryAPI, role, "river"); err != nil {
		t.Fatalf("the script-provisioned role failed readiness after the manifest was granted: %v", err)
	}
}

// CHAOS-6804 r1 P1: the login must BE the role, not merely act as it. With a
// startup option `-c role=<query-api role>` the session authenticates as another
// login (session_user) while current_user reads as the least-privilege role; a
// posture check that only reads current_user passes a DSN that still carries the
// owner's or a superuser's credential (which can RESET ROLE).
func TestCheckQueryAPIAuthorizationRefusesALoginThatOnlyActsAsTheRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_actsas")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)

	// A different login that is a member of the role and connects with
	// options=-c role=<role>.
	other, err := containers.RoleName("qapi_actsas_login", fixture.inst)
	if err != nil {
		t.Fatal(err)
	}
	dbName := fixture.dbName
	for _, statement := range []string{
		"CREATE ROLE " + other + " LOGIN SUPERUSER PASSWORD '" + apiAuthorizationPass + "'",
		"GRANT " + role + " TO " + other,
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + other,
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(fixture.admin, other, t.Logf) })
	config, err := pgxpool.ParseConfig(fixture.uri)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = other, apiAuthorizationPass
	config.ConnConfig.RuntimeParams["role"] = role
	actsAs, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(actsAs.Close)

	var sessionUser, currentUser string
	if err := actsAs.QueryRow(ctx, "SELECT session_user, current_user").Scan(&sessionUser, &currentUser); err != nil {
		t.Fatal(err)
	}
	if sessionUser != other || currentUser != role {
		t.Fatalf("the plant did not produce a session acting as the role: session_user=%q current_user=%q", sessionUser, currentUser)
	}
	err = CheckQueryAPIAuthorization(ctx, actsAs, role, "river")
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a login that only acts as the role (session_user=%s) passed the posture check: error %v", sessionUser, err)
	}
	if strings.Contains(err.Error(), apiAuthorizationPass) {
		t.Fatalf("the refusal leaks a credential: %v", err)
	}
	if err := CheckQueryAPIAuthorization(ctx, fixture.connect(t, ctx, role), role, "river"); err != nil {
		t.Fatalf("the control (a direct login as the role) failed: %v", err)
	}
}

// CHAOS-6804 r1 P1: "holds exactly the manifest" must not stop at the public and
// River schemas. A privilege on ANY relation or sequence in another schema, or
// CREATE on one, is a privilege outside the manifest.
func TestCheckQueryAPIAuthorizationRefusesPrivilegesOutsideTheManagedSchemas(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	for _, statement := range []string{
		"CREATE SCHEMA third_party",
		"CREATE TABLE third_party.secrets (id int)",
		"CREATE SEQUENCE third_party.counter",
		"CREATE VIEW third_party.secrets_view AS SELECT id FROM third_party.secrets",
	} {
		if _, err := fixture.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	cases := []struct {
		name  string
		plant func(role string) []string
		named string
	}{
		{"SELECT on a table in another schema", func(r string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO " + r, "GRANT SELECT ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"a write on a table in another schema", func(r string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO " + r, "GRANT INSERT ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"SELECT on a view in another schema", func(r string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO " + r, "GRANT SELECT ON third_party.secrets_view TO " + r}
		}, "third_party.secrets_view"},
		{"USAGE on a sequence in another schema", func(r string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO " + r, "GRANT USAGE ON SEQUENCE third_party.counter TO " + r}
		}, "third_party.counter"},
		{"a PUBLIC grant on a table in another schema", func(string) []string {
			return []string{"GRANT USAGE ON SCHEMA third_party TO PUBLIC", "GRANT SELECT ON third_party.secrets TO PUBLIC"}
		}, "third_party.secrets"},
		{"TRUNCATE alone on a table in another schema", func(r string) []string {
			return []string{"GRANT TRUNCATE ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"TRIGGER alone on a table in another schema", func(r string) []string {
			return []string{"GRANT TRIGGER ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"REFERENCES alone on a table in another schema", func(r string) []string {
			return []string{"GRANT REFERENCES ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"a COLUMN-level SELECT on a table in another schema", func(r string) []string {
			return []string{"GRANT SELECT (id) ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"a COLUMN-level UPDATE on a table in another schema", func(r string) []string {
			return []string{"GRANT UPDATE (id) ON third_party.secrets TO " + r}
		}, "third_party.secrets"},
		{"CREATE on another schema", func(r string) []string {
			return []string{"GRANT CREATE ON SCHEMA third_party TO " + r}
		}, "third_party"},
	}
	for index, test := range cases {
		role := fixture.newRole(t, ctx, fmt.Sprintf("_o%d", index))
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
		err := CheckQueryAPIAuthorization(ctx, pool, role, "river")
		if !errors.Is(err, ErrPostureRefused) || !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: error %v, want ErrUnavailable+ErrPostureRefused", test.name, err)
		} else if !strings.Contains(err.Error(), test.named) {
			t.Errorf("%s: the refusal must name %s: %v", test.name, test.named, err)
		}
		for _, statement := range []string{
			"REVOKE ALL ON SCHEMA third_party FROM PUBLIC",
			"REVOKE ALL ON ALL TABLES IN SCHEMA third_party FROM PUBLIC",
		} {
			if _, err := fixture.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: undo %q: %v", test.name, statement, err)
			}
		}
	}
}

// CHAOS-6937 r1 P1: the query-api check is a sequence of statements (identity,
// ownership, the whole-catalog grant enumeration, name resolution), and the
// enumeration is the longest of them. It must run under the same server-side
// statement_timeout as the other roles' posture checks: with the bound made
// impossibly short, the check reports "the database never answered"
// (ErrUnavailable carrying SQLSTATE 57014), never a refusal and never a pass;
// with the real bound the same role passes, so the timeout is what stopped it.
func TestCheckQueryAPIAuthorizationRunsUnderTheServerSideStatementTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_timeout")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	pool := fixture.connect(t, ctx, role)
	if err := CheckQueryAPIAuthorization(ctx, pool, role, "river"); err != nil {
		t.Fatalf("control: the role holding exactly the manifest failed readiness: %v", err)
	}

	previous := rolePostureStatementTimeout
	rolePostureStatementTimeout = time.Millisecond
	t.Cleanup(func() { rolePostureStatementTimeout = previous })
	err := CheckQueryAPIAuthorization(ctx, pool, role, "river")
	var pgErr *pgconn.PgError
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPostureRefused) || !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("query-api check under a 1 ms statement_timeout = %v, want ErrUnavailable carrying SQLSTATE 57014", err)
	}
}
