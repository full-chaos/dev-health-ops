//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"net/url"
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

// A pre-existing login that is not an eligible unprivileged login (CREATEDB here) is
// REFUSED before anything changes (the script left it alone; deliberate deviation, r3
// P1), naming the label; --check on roles that do not exist yet fails and changes nothing.
func TestMigrateRolesRefusesAnOverPrivilegedPreExistingRoleBeforeChangingAnything(t *testing.T) {
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
	if code != 1 || !strings.Contains(stderr.String(), "not an unprivileged login") || !strings.Contains(stderr.String()+stdout.String(), "domain") {
		t.Fatalf("an over-privileged pre-existing role must be refused up front: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, leaked := range []string{"new-secret", "pre_domain", "pre_queue", "pre_coordinator"} {
		if strings.Contains(output, leaked) {
			t.Fatalf("the output leaked %q", leaked)
		}
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname IN ('pre_queue', 'pre_coordinator')`).Scan(&created); err != nil || created != 0 {
		t.Fatalf("the refused run must have created nothing: %d %v", created, err)
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

// r2 P1-b: no role name (which Compose defaults to the role's own PASSWORD) may reach
// the output when provisioning fails.
func TestMigrateRolesFailureNeverPrintsAConfiguredRoleNameOrPassword(t *testing.T) {
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
	// A migration login that can connect but may not CREATE ROLE.
	if _, err := admin.Exec(ctx, `CREATE ROLE weak_migrator LOGIN PASSWORD 'weak-pw'`); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword("weak_migrator", "weak-pw")
	weakURI := parsed.String()
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              weakURI,
		"RIVER_DOMAIN_DATABASE_ROLE":          "review_secret",
		"RIVER_QUEUE_DATABASE_ROLE":           "queue_secret",
		"RIVER_COORDINATOR_DATABASE_ROLE":     "coordinator_secret",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "review_secret",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "queue_secret",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "coordinator_secret",
	}
	lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("a migration login that cannot CREATE ROLE must fail: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, secret := range []string{"review_secret", "queue_secret", "coordinator_secret"} {
		if strings.Contains(output, secret) {
			t.Errorf("the failure output contains %q (a role name that is also a password):\n%s", secret, output)
		}
	}
	if !strings.Contains(output, "domain") {
		t.Errorf("the failure must still name the role LABEL:\n%s", output)
	}
}

// r2 P1-c: an existing login keeps its password (never rotated) and a supplied
// password that does not authenticate fails the command, naming the role label.
func TestMigrateRolesFailsWhenAnExistingLoginHasADifferentPasswordAndChangesNothing(t *testing.T) {
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
	if _, err := admin.Exec(ctx, `CREATE ROLE stale_domain LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'old-domain-pass'`); err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          "stale_domain",
		"RIVER_QUEUE_DATABASE_ROLE":           "stale_queue",
		"RIVER_COORDINATOR_DATABASE_ROLE":     "stale_coordinator",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "new-domain-pass",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "queue-pass-ok",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "coordinator-pass-ok",
	}
	lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := rivermigrate.ExecuteRoles(ctx, "dho", nil, lookup, &stdout, &stderr)
	output := stdout.String() + stderr.String()
	if code != 1 || !strings.Contains(output, "does not authenticate") {
		t.Fatalf("a stale password must fail the command: exit %d\n%s", code, output)
	}
	for _, secret := range []string{"old-domain-pass", "new-domain-pass", "queue-pass-ok", "coordinator-pass-ok"} {
		if strings.Contains(output, secret) {
			t.Errorf("a password leaked into the output: %q", secret)
		}
	}
	login := func(user, password string) bool {
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.User, config.ConnConfig.Password = user, password
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return false
		}
		defer pool.Close()
		return pool.Ping(ctx) == nil
	}
	if !login("stale_domain", "old-domain-pass") || login("stale_domain", "new-domain-pass") {
		t.Fatal("the existing role's password must be left exactly as it was")
	}
	if !login("stale_queue", "queue-pass-ok") {
		t.Fatal("the roles the command created must authenticate with the supplied passwords")
	}
	// --check reports the same, and changes nothing.
	stdout.Reset()
	stderr.Reset()
	if code := rivermigrate.ExecuteRoles(ctx, "dho", []string{"--check"}, lookup, &stdout, &stderr); code != 1 ||
		!strings.Contains(stdout.String()+stderr.String(), "does not authenticate") {
		t.Fatalf("--check must report the stale password: exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	for _, leaked := range []string{"stale_domain", "stale_queue", "stale_coordinator", "old-domain-pass", "new-domain-pass", "queue-pass-ok", "coordinator-pass-ok"} {
		if strings.Contains(stdout.String()+stderr.String(), leaked) {
			t.Errorf("--check output leaked %q (role names and passwords never appear, only labels)", leaked)
		}
	}
}
