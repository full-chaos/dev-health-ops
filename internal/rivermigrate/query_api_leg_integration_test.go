//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The query-api role leg through the real command: with QUERY_API_DATABASE_ROLE
// unset the migration grants that role nothing, with it naming an existing role
// the migration applies the declared manifest (CHAOS-6804: REVOKE ALL then
// GRANT), with it naming a role that does not exist the leg is skipped with a
// warning, and with it naming a role that has a full posture of its own, or one
// that OWNS an object, the command is refused before any statement runs.
//
// The database here holds only the four tables the leg grants on, so the
// command's closing executed check of the DOMAIN, QUEUE and COORDINATOR
// postures (which needs the whole application schema) reports their missing
// tables and the command exits 1 after its grants committed. That is the one
// failure `applied` tolerates; it says nothing about the query-api leg.
func TestMigrateRiverAppliesTheQueryAPILegOnlyWhenNamed(t *testing.T) {
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

	domain, queue, coordinator := randomName(t, "qa_domain_"), randomName(t, "qa_queue_"), randomName(t, "qa_coord_")
	queryAPI, absent := randomName(t, "qa_query_"), randomName(t, "qa_absent_")
	for _, role := range []string{domain, queue, coordinator, queryAPI} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, role := range []string{domain, queue, coordinator, queryAPI} {
			_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
			_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		}
	})
	for _, table := range []string{"saved_reports", "scheduled_jobs", "report_runs", "worker_job_outbox"} {
		if _, err := admin.Exec(ctx, "CREATE TABLE public."+table+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
	}
	base := map[string]string{
		"MIGRATION_DATABASE_URI":          instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":      domain,
		"RIVER_QUEUE_DATABASE_ROLE":       queue,
		"RIVER_COORDINATOR_DATABASE_ROLE": coordinator,
	}
	const postureGate = "executed grant posture check found missing privileges"
	applied := func(code int, stderr string) bool {
		return code == cli.ExitOK || (code == 1 && strings.Contains(stderr, postureGate))
	}
	migrate := func(extra map[string]string) (int, string) {
		settings := map[string]string{}
		for key, value := range base {
			settings[key] = value
		}
		for key, value := range extra {
			settings[key] = value
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		return rivermigrate.Execute(ctx, "dho", nil, lookup, &stdout, &stderr), stderr.String()
	}
	held := func(table, privilege string) bool {
		var ok bool
		if err := admin.QueryRow(ctx, "SELECT has_table_privilege($1, $2, $3)", queryAPI, "public."+table, privilege).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	anyHeld := func() bool {
		for _, table := range []string{"saved_reports", "scheduled_jobs", "report_runs", "worker_job_outbox"} {
			for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
				if held(table, privilege) {
					return true
				}
			}
		}
		return false
	}

	if code, stderr := migrate(nil); !applied(code, stderr) || anyHeld() {
		t.Fatalf("no query-api role named: exit %d, the role holds a privilege (%v), stderr:\n%s", code, anyHeld(), stderr)
	}
	if code, stderr := migrate(map[string]string{"QUERY_API_DATABASE_ROLE": absent}); !applied(code, stderr) ||
		!strings.Contains(stderr, "query-api Postgres role does not exist; query-api grants skipped") {
		t.Fatalf("a role that does not exist must be skipped with a warning: exit %d, stderr:\n%s", code, stderr)
	}
	if code, stderr := migrate(map[string]string{"QUERY_API_DATABASE_ROLE": domain}); code != 1 ||
		!strings.Contains(stderr, "must be distinct") || strings.Contains(stderr, postureGate) {
		t.Fatalf("naming the domain role as the query-api role must be refused before anything runs: exit %d, stderr:\n%s", code, stderr)
	}
	// A role that owns an object (the registry owner query-api logs in as today)
	// must be refused, not REVOKEd: the leg would strip it of its own tables.
	ownerRole := randomName(t, "qa_owner_")
	for _, statement := range []string{
		"CREATE ROLE " + ownerRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'x'",
		"CREATE TABLE public.qa_owned_by_role (id uuid PRIMARY KEY)",
		"ALTER TABLE public.qa_owned_by_role OWNER TO " + ownerRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+ownerRole)
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+ownerRole)
	})
	if code, stderr := migrate(map[string]string{"QUERY_API_DATABASE_ROLE": ownerRole}); code != 1 || strings.Contains(stderr, postureGate) {
		t.Fatalf("naming a role that owns a table must be refused before any grant: exit %d, stderr:\n%s", code, stderr)
	}
	if anyHeld() {
		t.Fatal("a skipped or refused leg granted the query-api role something")
	}

	if code, stderr := migrate(map[string]string{"QUERY_API_DATABASE_ROLE": queryAPI}); !applied(code, stderr) {
		t.Fatalf("the query-api leg: exit %d, stderr:\n%s", code, stderr)
	}
	for table, want := range map[string]map[string]bool{
		"saved_reports":     {"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true},
		"scheduled_jobs":    {"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": false},
		"report_runs":       {"SELECT": true, "INSERT": true, "UPDATE": false, "DELETE": false},
		"worker_job_outbox": {"SELECT": true, "INSERT": true, "UPDATE": false, "DELETE": false},
	} {
		for privilege, wanted := range want {
			if got := held(table, privilege); got != wanted {
				t.Errorf("%s %s held=%v, want %v", table, privilege, got, wanted)
			}
		}
		if held(table, "TRUNCATE") {
			t.Errorf("%s: TRUNCATE must never be granted", table)
		}
	}
}
