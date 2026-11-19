//go:build integration

package riverstore_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The post_sync outcome stamp in the prod role shape: a NOSUPERUSER migration login owns the database and builds the
// schema (baseline and chain, revision 0166 included), so it owns sync_run_post_dispatches as it does after the owner
// move, and it applies the runtime grants. The domain role then holds UPDATE on post_sync_outcome and
// post_sync_committed_at only, granted by the migration login, and every other ledger column refuses an UPDATE (42501).
func TestPostSyncOutcomeGrantAsTheNosuperuserOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)
	domainRole, err := containers.RoleName("outcome_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("outcome_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := containers.RoleName("outcome_migrator", instance)
	if err != nil {
		t.Fatal(err)
	}
	database := mustDatabaseName(t, instance.URI)
	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	defer containers.DropRole(adminPool, migrator, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	const migratorPassword = "outcome_migrator_test_password"
	for _, statement := range []string{
		"CREATE ROLE " + migrator + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + migratorPassword + "'",
		"ALTER DATABASE " + pgx.Identifier{database}.Sanitize() + " OWNER TO " + migrator,
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), "ALTER DATABASE "+pgx.Identifier{database}.Sanitize()+" OWNER TO CURRENT_USER")
		_, _ = adminPool.Exec(context.Background(), "DROP OWNED BY "+migrator+" CASCADE")
	}()

	migratorURI := roleURI(t, instance.URI, migrator, migratorPassword, database)
	migratorConn, err := pgx.Connect(ctx, migratorURI)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migratorConn.Close(context.Background()) }()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, migratorConn, baseline, chain); err != nil {
		t.Fatalf("baseline and chain as the NOSUPERUSER owner: %v", err)
	}
	migratorPool := openPool(t, ctx, migratorURI)
	defer migratorPool.Close()
	if _, err := riverstore.ApplyPinnedMigrations(ctx, migratorPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatalf("runtime grants as the NOSUPERUSER owner: %v", err)
	}

	var owner string
	var superuser bool
	if err := adminPool.QueryRow(ctx, `SELECT pg_get_userbyid(c.relowner), r.rolsuper FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
		WHERE c.oid = 'public.sync_run_post_dispatches'::regclass`).Scan(&owner, &superuser); err != nil {
		t.Fatal(err)
	}
	if owner != migrator || superuser {
		t.Fatalf("sync_run_post_dispatches owner = %s (superuser %v), want the NOSUPERUSER migration login %s", owner, superuser, migrator)
	}
	acl := func(query string) []string {
		t.Helper()
		rows, err := adminPool.Query(ctx, query, domainRole)
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return lines
	}
	columnACL := acl(`SELECT t.attname || ' ' || a.grantor::regrole::text || '>' || a.grantee::regrole::text || ' ' || a.privilege_type ||
			CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END
		FROM pg_attribute t, LATERAL aclexplode(t.attacl) a
		WHERE t.attrelid = 'public.sync_run_post_dispatches'::regclass AND t.attacl IS NOT NULL AND a.grantee = $1::regrole
		ORDER BY 1`)
	wantColumns := []string{
		"post_sync_committed_at " + migrator + ">" + domainRole + " UPDATE",
		"post_sync_outcome " + migrator + ">" + domainRole + " UPDATE",
	}
	if strings.Join(columnACL, "\n") != strings.Join(wantColumns, "\n") {
		t.Errorf("domain role column grants on sync_run_post_dispatches:\n%s\nwant\n%s", strings.Join(columnACL, "\n"), strings.Join(wantColumns, "\n"))
	}
	tableACL := acl(`SELECT a.grantor::regrole::text || '>' || a.grantee::regrole::text || ' ' || a.privilege_type
		FROM pg_class c, LATERAL aclexplode(c.relacl) a
		WHERE c.oid = 'public.sync_run_post_dispatches'::regclass AND a.grantee = $1::regrole
		ORDER BY 1`)
	wantTable := []string{migrator + ">" + domainRole + " INSERT", migrator + ">" + domainRole + " SELECT"}
	if strings.Join(tableACL, "\n") != strings.Join(wantTable, "\n") {
		t.Errorf("domain role table grants on sync_run_post_dispatches:\n%s\nwant\n%s", strings.Join(tableACL, "\n"), strings.Join(wantTable, "\n"))
	}

	gaps, err := postgresstore.DiagnoseRolePosture(ctx, adminPool, domainRole, postgresstore.DomainPosture())
	if err != nil {
		t.Fatal(err)
	}
	for _, gap := range gaps {
		if gap.TableName == "sync_run_post_dispatches" {
			t.Errorf("the readiness check disagrees with what the migrator granted on sync_run_post_dispatches: %+v", gap)
		}
	}

	domainPool := openPool(t, ctx, roleURI(t, instance.URI, domainRole, domainPassword, database))
	defer domainPool.Close()
	if _, err := domainPool.Exec(ctx, `UPDATE public.sync_run_post_dispatches SET post_sync_outcome = 'published', post_sync_committed_at = now()
		WHERE sync_run_id = gen_random_uuid() AND post_sync_outcome IS NULL`); err != nil {
		t.Errorf("the outcome stamp as the domain role: %v, want it allowed", err)
	}
	for column, value := range map[string]string{
		"id": "gen_random_uuid()", "org_id": "'org'", "sync_run_id": "gen_random_uuid()", "kind": "'post_sync'",
		"dispatched_at": "now()", "recorded_at": "now()",
	} {
		_, err := domainPool.Exec(ctx, `UPDATE public.sync_run_post_dispatches SET `+column+` = `+value+` WHERE sync_run_id = gen_random_uuid()`)
		if !isInsufficientPrivilege(err) {
			t.Errorf("UPDATE of %s as the domain role: %v, want 42501", column, err)
		}
	}
	if _, err := domainPool.Exec(ctx, `DELETE FROM public.sync_run_post_dispatches WHERE sync_run_id = gen_random_uuid()`); !isInsufficientPrivilege(err) {
		t.Errorf("DELETE as the domain role: %v, want 42501", err)
	}
}
