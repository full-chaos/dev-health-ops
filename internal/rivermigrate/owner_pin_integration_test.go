//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-12614: `dho migrate river --apply-and-check` as the NOSUPERUSER login that owns the database (the pinned
// migration login). It creates the River schema, applies River and the runtime grants, leaves query-api's auth
// session read to auth-grants (the auth schema belongs to the auth migrator) and revokes nothing there; a superuser DSN
// under the same pin is refused before any DDL.
func TestMigrateRiverAsThePinnedNosuperuserDatabaseOwner(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domain, queue, coordinator := randomName(t, "op_domain_"), randomName(t, "op_queue_"), randomName(t, "op_coord_")
	queryAPI, migrator, authOwner := randomName(t, "op_query_"), randomName(t, "op_migrator_"), randomName(t, "op_authmig_")
	roles := []string{domain, queue, coordinator, queryAPI, migrator, authOwner}
	for _, role := range roles {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "ALTER DATABASE "+pgx.Identifier{database}.Sanitize()+" OWNER TO CURRENT_USER")
		for _, role := range roles {
			_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role+" CASCADE")
			_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		}
	})
	asRole := func(role string) string {
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, "x")
		return parsed.String()
	}
	run := func(conn *pgx.Conn, statements ...string) {
		t.Helper()
		for _, statement := range statements {
			if _, err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	run(admin,
		"ALTER DATABASE "+pgx.Identifier{database}.Sanitize()+" OWNER TO "+migrator,
		"CREATE SCHEMA auth AUTHORIZATION "+authOwner,
	)
	migratorConn, err := pgx.Connect(ctx, asRole(migrator))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migratorConn.Close(context.Background()) })
	for _, table := range []string{"saved_reports", "scheduled_jobs", "report_runs", "worker_job_outbox"} {
		run(migratorConn, "CREATE TABLE public."+table+" (id uuid PRIMARY KEY)")
	}
	authConn, err := pgx.Connect(ctx, asRole(authOwner))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authConn.Close(context.Background()) })
	// What auth-grants (sessionReaders.queryApi) gives query-api, as the auth migrator.
	run(authConn,
		"CREATE TABLE auth.sessions (id uuid PRIMARY KEY, user_id uuid, expires_at timestamptz, revoked_at timestamptz, token_version integer)",
		"GRANT USAGE ON SCHEMA auth TO "+queryAPI,
		"GRANT SELECT (id, user_id) ON auth.sessions TO "+queryAPI,
	)

	const postureGate = "executed grant posture check found missing privileges"
	migrate := func(uri string) (int, string) {
		settings := map[string]string{
			"MIGRATION_DATABASE_URI":          uri,
			"RIVER_DOMAIN_DATABASE_ROLE":      domain,
			"RIVER_QUEUE_DATABASE_ROLE":       queue,
			"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
			"QUERY_API_DATABASE_ROLE":         queryAPI,
			"AUTH_PROXY_SESSIONS":             "true",
			"AUTH_DATABASE_SCHEMA":            "auth",
			ownermove.EnvOwnerRole:            migrator,
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return rivermigrate.Execute(ctx, "dho", []string{"--apply-and-check"}, lookup, &stdout, &stderr), stdout.String() + stderr.String()
	}
	riverExists := func() bool {
		var ok bool
		if err := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'river')").Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}

	authACL := func() []string {
		t.Helper()
		rows, err := admin.Query(ctx, `SELECT kind || ' ' || object || ' ' || a.grantor::regrole::text || '>' || a.grantee::regrole::text || ' ' || a.privilege_type
			FROM (SELECT 'schema' AS kind, n.nspname::text AS object, n.nspacl AS acl FROM pg_namespace n WHERE n.nspname = 'auth'
			      UNION ALL SELECT 'relation', c.relname::text, c.relacl FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'auth'
			      UNION ALL SELECT 'column', c.relname || '.' || t.attname, t.attacl FROM pg_attribute t JOIN pg_class c ON c.oid = t.attrelid
			        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'auth' AND t.attacl IS NOT NULL) o,
			LATERAL aclexplode(o.acl) a ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return lines
	}
	authBefore := authACL()

	if code, out := migrate(instance.URI); code != 1 || !strings.Contains(out, "ops schema owner pin") || riverExists() {
		t.Fatalf("a superuser DSN under the pin: exit %d, river created %v\n%s", code, riverExists(), out)
	}
	code, out := migrate(asRole(migrator))
	if !(code == cli.ExitOK || (code == 1 && strings.Contains(out, postureGate))) {
		t.Fatalf("the pinned NOSUPERUSER owner: exit %d\n%s", code, out)
	}
	for _, want := range []string{ownermove.PinVerifiedMessage, "query-api's session read is left to auth-grants"} {
		if !strings.Contains(out, want) {
			t.Errorf("the run did not log %q:\n%s", want, out)
		}
	}
	var riverOwner string
	var sessionRead, savedReports bool
	if err := admin.QueryRow(ctx, `SELECT (SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'river'),
		has_column_privilege($1, 'auth.sessions', 'user_id', 'SELECT'),
		has_table_privilege($1, 'public.saved_reports', 'DELETE')`, queryAPI).Scan(&riverOwner, &sessionRead, &savedReports); err != nil {
		t.Fatal(err)
	}
	if riverOwner != migrator {
		t.Errorf("schema river is owned by %s, want the migration login", riverOwner)
	}
	if !sessionRead {
		t.Error("the River leg revoked query-api's auth session read that auth-grants gave it")
	}
	if after := authACL(); strings.Join(after, "\n") != strings.Join(authBefore, "\n") {
		t.Errorf("the River leg as the pinned login changed ACL entries in schema auth:\nbefore %v\nafter  %v", authBefore, after)
	}
	if !savedReports {
		t.Error("the query-api leg did not grant its public manifest")
	}
	if err := ownermove.VerifyPin(ctx, migratorConn, migrator, "river"); err != nil {
		t.Fatalf("the pin after River as the migration login: %v", err)
	}
}
