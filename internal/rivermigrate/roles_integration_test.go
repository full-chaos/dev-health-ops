//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestMigrateRolesRefusesWhatWouldBeAmbiguousBeforeTouchingTheDatabase(t *testing.T) {
	ctx := context.Background()
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

	migrationLogin, err := postgresstore.ConnectionUser(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          "ref_domain",
		"RIVER_QUEUE_DATABASE_ROLE":           "ref_queue",
		"RIVER_COORDINATOR_DATABASE_ROLE":     "ref_coordinator",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "ref-secret-domain",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "ref-secret-queue",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "ref-secret-coordinator",
	}
	for name, mutate := range map[string]func(map[string]string){
		"a KEDA role named like the domain role": func(m map[string]string) {
			m["RIVER_KEDA_READONLY_DATABASE_ROLE"] = "ref_domain"
			m["RIVER_KEDA_READONLY_PASSWORD"] = "ref-secret-keda"
		},
		"two mandatory roles alike":             func(m map[string]string) { m["RIVER_QUEUE_DATABASE_ROLE"] = "ref_domain" },
		"a missing password":                    func(m map[string]string) { delete(m, "RIVER_QUEUE_DATABASE_PASSWORD") },
		"an optional role without its password": func(m map[string]string) { m["API_DATABASE_ROLE"] = "ref_api" },
		"the migration login as a runtime role": func(m map[string]string) { m["RIVER_DOMAIN_DATABASE_ROLE"] = migrationLogin },
		"a password and its _FILE both set":     func(m map[string]string) { m["RIVER_QUEUE_DATABASE_PASSWORD_FILE"] = "/nonexistent" },
	} {
		settings := map[string]string{}
		for key, value := range base {
			settings[key] = value
		}
		mutate(settings)
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr)
		if code != 1 {
			t.Errorf("%s: exit %d, want 1\nstdout:\n%s\nstderr:\n%s", name, code, stdout.String(), stderr.String())
		}
		for _, secret := range []string{"ref-secret-domain", "ref-secret-queue", "ref-secret-coordinator", "ref-secret-keda"} {
			if strings.Contains(stdout.String()+stderr.String(), secret) {
				t.Errorf("%s: a password leaked into the output", name)
			}
		}
	}
	var roles int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname LIKE 'ref\_%'`).Scan(&roles); err != nil {
		t.Fatal(err)
	}
	if roles != 0 {
		t.Fatalf("a refused invocation created %d role(s)", roles)
	}
}

// A role the command cannot make safe (a pre-existing login with CREATEDB is left
// alone, like the script) is NAMED by the closing check and fails the command; and
// --check changes nothing.
func TestMigrateRolesFailsLoudlyOnAnOverPrivilegedPreExistingRoleAndCheckChangesNothing(t *testing.T) {
	ctx := context.Background()
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
	if _, err := admin.Exec(ctx, `CREATE ROLE pre_domain LOGIN CREATEDB PASSWORD 'old'`); err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          "pre_domain",
		"RIVER_QUEUE_DATABASE_ROLE":           "pre_queue",
		"RIVER_COORDINATOR_DATABASE_ROLE":     "pre_coordinator",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "new-secret-domain",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "new-secret-queue",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "new-secret-coordinator",
	}
	lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }

	var stdout, stderr bytes.Buffer
	if code := rivermigrate.ExecuteRoles(ctx, "dho", []string{"--check"}, lookup, &stdout, &stderr); code != 1 {
		t.Fatalf("--check with roles that do not exist / are over-privileged must fail: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	var created int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname IN ('pre_queue', 'pre_coordinator')`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("--check created %d role(s): it must change nothing", created)
	}

	stdout.Reset()
	stderr.Reset()
	code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "do not meet the bootstrap postconditions") ||
		!strings.Contains(stderr.String()+stdout.String(), "domain") {
		t.Fatalf("an over-privileged pre-existing role must fail the closing check: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "new-secret") {
		t.Fatal("a password leaked into the output")
	}
	// The other two roles WERE provisioned (Apply committed before the check).
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname IN ('pre_queue', 'pre_coordinator')`).Scan(&created); err != nil || created != 2 {
		t.Fatalf("the apply step must have committed the other roles: %d %v", created, err)
	}
}

// r1/r1b P1: through the real command a role name is used EXACTLY as configured.
// A runtime role `dho migrate river` would refuse (spaces, upper case, dots) is
// refused UP FRONT, naming the rule and creating nothing; the KEDA login, which
// river never reads, is provisioned under exactly its odd name.
func TestMigrateRolesUsesRoleNamesExactlyAsConfiguredAndRefusesWhatRiverWouldRefuse(t *testing.T) {
	ctx := context.Background()
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
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          " Edge Domain \"quoted \"",
		"RIVER_QUEUE_DATABASE_ROLE":           "q_exact",
		"RIVER_COORDINATOR_DATABASE_ROLE":     "c_exact",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "exact-pw-1",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "exact-pw-2",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "exact-pw-3",
	}
	lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "[a-z_][a-z0-9_]*") {
		t.Fatalf("a runtime role river would refuse must be refused up front naming the rule: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	var created int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname IN ('q_exact', 'c_exact') OR rolname LIKE '%Edge%'`).Scan(&created); err != nil || created != 0 {
		t.Fatalf("a refused invocation created %d role(s): %v", created, err)
	}

	// The River schema exists (KEDA reads it); the runtime roles are valid; KEDA is odd.
	if _, err := admin.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE river.river_job (id bigint)"); err != nil {
		t.Fatal(err)
	}
	settings["RIVER_DOMAIN_DATABASE_ROLE"] = "d_exact"
	kedaName := ` Odd Keda "quoted "É.Name `
	settings["RIVER_KEDA_READONLY_DATABASE_ROLE"] = kedaName
	settings["RIVER_KEDA_READONLY_PASSWORD"] = "exact-pw-keda"
	stdout.Reset()
	stderr.Reset()
	if code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr); code != cli.ExitOK {
		t.Fatalf("migrate roles: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	for _, name := range []string{"d_exact", "q_exact", "c_exact", kedaName} {
		var exact bool
		if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, name).Scan(&exact); err != nil || !exact {
			t.Errorf("no role named exactly %q was created: %v", name, err)
		}
	}
}
