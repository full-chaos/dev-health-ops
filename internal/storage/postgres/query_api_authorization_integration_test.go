//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const queryAPIAuthorizationRole = "devhealth_query_api_authorization_test"

// queryAPIFixture starts one Postgres with stand-ins for the four tables the
// additive manifest names plus an unrelated one. Each test then makes as many
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
	for _, table := range QueryAPIWritePosture().RequiredTables {
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
	for _, table := range QueryAPIWritePosture().RequiredTables {
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

// The role holds exactly the declared writes: ready. Holding MORE (an
// unrelated table, the read plane it lives on today) must not refuse it, which
// is what makes this check additive rather than a least-privilege posture.
func TestCheckQueryAPIWriteGrantsAcceptsTheDeclaredWritesAndAnythingMore(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	admin, role := fixture.admin, fixture.newRole(t, ctx, "_ok")
	grantQueryAPIManifest(t, ctx, admin, role, nil)
	if _, err := admin.Exec(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON unrelated_read_table TO "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT TRUNCATE ON saved_reports TO "+role); err != nil {
		t.Fatal(err)
	}
	if err := CheckQueryAPIWriteGrants(ctx, fixture.connect(t, ctx, role), role); err != nil {
		t.Fatalf("a role holding the declared writes (and more) failed readiness: %v", err)
	}
}

// Each declared privilege, withheld alone, refuses readiness and names the
// table: the check is per (table, privilege), not per table.
func TestCheckQueryAPIWriteGrantsRefusesEveryMissingDeclaredPrivilege(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	cases := 0
	for _, table := range QueryAPIWritePosture().RequiredTables {
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
			err := CheckQueryAPIWriteGrants(ctx, fixture.connect(t, ctx, role), role)
			if !errors.Is(err, ErrPostureRefused) || !errors.Is(err, ErrUnavailable) {
				t.Errorf("missing %s on %s: error %v, want ErrUnavailable+ErrPostureRefused", privilege, table.TableName, err)
				continue
			}
			if !strings.Contains(err.Error(), table.TableName) {
				t.Errorf("the refusal for missing %s on %s must name the table: %v", privilege, table.TableName, err)
			}
		}
	}
	// 4 tables: SELECT on each, plus 3+2+1+1 write flags.
	if cases != 4+7 {
		t.Fatalf("exercised %d (table, privilege) cases, want 11: the manifest changed without this test", cases)
	}
}

// Grants on a role the pool does not log in as are not the pool's grants.
func TestCheckQueryAPIWriteGrantsRefusesALoginThatIsNotTheNamedRole(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_login")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	// The admin pool logs in as the container's superuser, not the named role.
	err := CheckQueryAPIWriteGrants(ctx, fixture.admin, role)
	if !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a pool logged in as another role: error %v, want ErrPostureRefused", err)
	}
	if err := CheckQueryAPIWriteGrants(ctx, fixture.connect(t, ctx, role), role); err != nil {
		t.Fatalf("the same grants over the named login: %v", err)
	}
	// A name that is not a valid runtime identifier is refused as unusable
	// configuration, before the login is even compared: it is not a posture
	// refusal of a role that exists.
	err = CheckQueryAPIWriteGrants(ctx, fixture.admin, "Not A Role; DROP TABLE x")
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPostureRefused) {
		t.Fatalf("an invalid role name: error %v, want ErrUnavailable without ErrPostureRefused", err)
	}
}

// A declared table that does not exist is a gap, not a pass.
func TestCheckQueryAPIWriteGrantsRefusesAMissingTable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := startQueryAPIFixture(t)
	role := fixture.newRole(t, ctx, "_notable")
	grantQueryAPIManifest(t, ctx, fixture.admin, role, nil)
	if _, err := fixture.admin.Exec(ctx, "DROP TABLE report_runs"); err != nil {
		t.Fatal(err)
	}
	err := CheckQueryAPIWriteGrants(ctx, fixture.connect(t, ctx, role), role)
	if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "report_runs") {
		t.Fatalf("a missing declared table: error %v", err)
	}
}

// The whole chain on production-shaped tables: the real migrate leg grants the
// declared writes to a role that already reads more, readiness turns ready,
// and the statements the saved-report mutations issue succeed as that role
// while everything the manifest does not name is denied.
func TestQueryAPIWriteGrantsEndToEndThroughTheMigrateLeg(t *testing.T) {
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
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + role,
		// What the role already holds today: reads, plus a privilege the
		// manifest never names. The additive leg must leave both alone.
		"GRANT SELECT ON public.integrations TO " + role,
		"GRANT TRUNCATE ON public.sync_runs TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	queryAPI := connectAs(t, ctx, uri, role, password)

	if err := CheckQueryAPIWriteGrants(ctx, queryAPI, role); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("before the migrate leg the role holds no write grants: error %v", err)
	}

	grants := make([]riverstore.TableGrant, 0)
	for _, table := range QueryAPIWritePosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: grantSchema, DomainRole: roles.domain, QueueRole: roles.queue,
		QueryAPIRole: role, QueryAPIWriteGrants: grants,
	}); err != nil {
		t.Fatalf("ApplyPinnedMigrations with the query-api leg: %v", err)
	}
	if err := CheckQueryAPIWriteGrants(ctx, queryAPI, role); err != nil {
		t.Fatalf("after the migrate leg the role must be ready: %v", err)
	}

	// Additive: nothing the role held before was taken away.
	for _, check := range []struct{ table, privilege string }{
		{"public.integrations", "SELECT"}, {"public.sync_runs", "TRUNCATE"},
	} {
		var held bool
		if err := admin.QueryRow(ctx, "SELECT has_table_privilege($1, $2, $3)", role, check.table, check.privilege).Scan(&held); err != nil || !held {
			t.Errorf("the migrate leg removed %s on %s from the query-api role (held=%v, err=%v)", check.privilege, check.table, held, err)
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
		"TRUNCATE public.saved_reports",
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
