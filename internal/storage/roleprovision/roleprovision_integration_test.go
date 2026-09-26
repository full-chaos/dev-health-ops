//go:build integration

package roleprovision

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// CHAOS-6901: `dho migrate roles` replaces scripts/worker/provision_river_roles.sql.
// These tests run the REAL script through psql on one PostgreSQL and the Go leg on
// a second, identical one, then compare everything the script's roles can hold:
// role attributes, whether a password was set and works, every effective grant
// (roleacl's own enumeration, the one the readiness checks use) and the ACLs of the
// objects the script touches (the database, schema public, the River schema and
// river_job). CREATE ROLE is cluster-wide, so each side gets its own container.

func scriptPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve this test file's path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "scripts", "worker", "provision_river_roles.sql")
}

type side struct {
	instance *containers.Instance
	admin    *pgxpool.Pool
}

// startSide brings up one PostgreSQL with the River schema (KEDA reads river_job).
func startSide(t *testing.T) *side {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(admin), &rivermigrate.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Schema: "river",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	return &side{instance: instance, admin: admin}
}

func testOptions(withOptional bool) Options {
	options := Options{
		Domain:      Role{"parity_domain", "parity-domain-pass"},
		Queue:       Role{"parity_queue", "parity-queue-pass"},
		Coordinator: Role{"parity_coordinator", "parity-coordinator-pass"},
	}
	if withOptional {
		options.API = Role{"parity_api", "parity-api-pass"}
		options.QueryAPI = Role{"parity_query_api", "parity-query-api-pass"}
		options.Keda = Role{"parity_keda", "parity-keda-pass"}
		options.RiverSchema = "river"
	}
	return options
}

func (s *side) runScript(t *testing.T, options Options) {
	t.Helper()
	args := []string{
		s.instance.URI,
		"--set=ON_ERROR_STOP=1",
		"--set=domain_role=" + options.Domain.Name, "--set=domain_password=" + options.Domain.Password,
		"--set=queue_role=" + options.Queue.Name, "--set=queue_password=" + options.Queue.Password,
		"--set=coordinator_role=" + options.Coordinator.Name, "--set=coordinator_password=" + options.Coordinator.Password,
	}
	if options.API.Name != "" {
		args = append(args, "--set=api_role="+options.API.Name, "--set=api_password="+options.API.Password)
	}
	if options.QueryAPI.Name != "" {
		args = append(args, "--set=query_api_role="+options.QueryAPI.Name, "--set=query_api_password="+options.QueryAPI.Password)
	}
	if options.Keda.Name != "" {
		args = append(args, "--set=keda_role="+options.Keda.Name, "--set=keda_password="+options.Keda.Password,
			"--set=river_schema="+options.schema())
	}
	args = append(args, "--file="+scriptPath(t))
	if output, err := exec.Command("psql", args...).CombinedOutput(); err != nil {
		t.Fatalf("psql provision_river_roles.sql: %v\n%s", err, output)
	}
}

func (s *side) runGo(t *testing.T, options Options) {
	t.Helper()
	if err := Apply(context.Background(), s.admin, options); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// snapshot is everything observable about the provisioned roles, as sorted text.
func (s *side) snapshot(t *testing.T, options Options) []string {
	t.Helper()
	ctx := context.Background()
	var lines []string
	for _, entry := range options.configured() {
		var attributes string
		if err := s.admin.QueryRow(ctx, `
			SELECT format('super=%s inherit=%s createrole=%s createdb=%s login=%s replication=%s bypassrls=%s connlimit=%s valid=%s config=%s scram=%s',
				rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
				COALESCE(rolvaliduntil::text, 'null'), COALESCE(rolconfig::text, 'null'),
				(SELECT COALESCE(rolpassword, '') LIKE 'SCRAM-SHA-256$%' FROM pg_authid WHERE pg_authid.oid = pg_roles.oid))
			FROM pg_roles WHERE rolname = $1`, entry.role.Name).Scan(&attributes); err != nil {
			t.Fatalf("read attributes of %s: %v", entry.label, err)
		}
		lines = append(lines, entry.label+" attributes "+attributes)
		grants, err := roleacl.Enumerate(ctx, s.admin, entry.role.Name)
		if err != nil {
			t.Fatalf("enumerate %s: %v", entry.label, err)
		}
		for _, grant := range grants {
			lines = append(lines, entry.label+" grant "+grant.String())
		}
		var members int
		if err := s.admin.QueryRow(ctx, `SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = $1`, entry.role.Name).Scan(&members); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("%s memberships %d", entry.label, members))
		// The password works: the role can log in with it.
		lines = append(lines, fmt.Sprintf("%s login %v", entry.label, s.canLogin(entry.role)))
	}
	// The ACLs of the objects the script touches, in one normalised form with the
	// role oids replaced by names (the two clusters assign different oids).
	for _, query := range []string{
		`SELECT 'database' || ' ' || pg_get_userbyid(a.grantor) || '>' || COALESCE(NULLIF(pg_get_userbyid(a.grantee), ''), 'PUBLIC') || ' ' || a.privilege_type || ' ' || a.is_grantable
		   FROM pg_database d, aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a WHERE d.datname = current_database()`,
		`SELECT 'schema ' || n.nspname || ' ' || pg_get_userbyid(a.grantor) || '>' || COALESCE(NULLIF(pg_get_userbyid(a.grantee), ''), 'PUBLIC') || ' ' || a.privilege_type || ' ' || a.is_grantable
		   FROM pg_namespace n, aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a WHERE n.nspname IN ('public', $1)`,
		`SELECT 'river_job ' || pg_get_userbyid(a.grantor) || '>' || COALESCE(NULLIF(pg_get_userbyid(a.grantee), ''), 'PUBLIC') || ' ' || a.privilege_type || ' ' || a.is_grantable
		   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a
		  WHERE n.nspname = $1 AND c.relname = 'river_job'`,
	} {
		var args []any
		if strings.Contains(query, "$1") {
			args = []any{options.schema()}
		}
		rows, err := s.admin.Query(ctx, query, args...)
		if err != nil {
			t.Fatalf("read acl: %v", err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, "acl "+line)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	sort.Strings(lines)
	return lines
}

func (s *side) canLogin(role Role) bool {
	config, err := pgx.ParseConfig(s.instance.URI)
	if err != nil {
		return false
	}
	config.User, config.Password = role.Name, role.Password
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return false
	}
	_ = connection.Close(ctx)
	return true
}

func diff(t *testing.T, name string, script, golang []string) {
	t.Helper()
	only := func(a, b []string) []string {
		set := map[string]bool{}
		for _, line := range b {
			set[line] = true
		}
		var out []string
		for _, line := range a {
			if !set[line] {
				out = append(out, line)
			}
		}
		return out
	}
	if a, b := only(script, golang), only(golang, script); len(a)+len(b) > 0 {
		t.Errorf("%s: the Go leg and the psql script disagree.\n  only the script:\n    %s\n  only the Go leg:\n    %s",
			name, strings.Join(a, "\n    "), strings.Join(b, "\n    "))
	}
	if len(script) == 0 {
		t.Errorf("%s: the snapshot is empty, so the comparison proves nothing", name)
	}
}

func TestGoLegMatchesTheScriptForTheMandatoryRoles(t *testing.T) {
	t.Parallel()
	options := testOptions(false)
	script, golang := startSide(t), startSide(t)
	script.runScript(t, options)
	golang.runGo(t, options)
	diff(t, "mandatory roles", script.snapshot(t, options), golang.snapshot(t, options))
	problems, _, err := Verify(context.Background(), golang.admin, options)
	if err != nil || len(problems) != 0 {
		t.Fatalf("Verify on the Go leg's result: %v %v", problems, err)
	}
}

func TestGoLegMatchesTheScriptForEveryOptionalRole(t *testing.T) {
	t.Parallel()
	options := testOptions(true)
	script, golang := startSide(t), startSide(t)
	script.runScript(t, options)
	golang.runGo(t, options)
	scriptSnapshot, goSnapshot := script.snapshot(t, options), golang.snapshot(t, options)
	diff(t, "all roles", scriptSnapshot, goSnapshot)
	// The comparison must actually contain the interesting facts, not agree on nothing.
	joined := strings.Join(goSnapshot, "\n")
	for _, want := range []string{
		"keda grant", "keda login true", "api login true", "query_api login true",
		"acl river_job", "domain attributes super=f",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the snapshot lacks %q, so the parity claim would be vacuous:\n%s", want, joined)
		}
	}
	problems, warnings, err := Verify(context.Background(), golang.admin, options)
	if err != nil || len(problems) != 0 || len(warnings) != 0 {
		t.Fatalf("Verify: problems=%v warnings=%v err=%v", problems, warnings, err)
	}
}

func TestGoLegIsIdempotentAndRotatesOnlyTheKedaPassword(t *testing.T) {
	t.Parallel()
	options := testOptions(true)
	script, golang := startSide(t), startSide(t)
	script.runScript(t, options)
	golang.runGo(t, options)
	first := golang.snapshot(t, options)
	golang.runGo(t, options)
	diff(t, "second Go run vs first", first, golang.snapshot(t, options))

	// A second run with new passwords: only KEDA's is re-applied, on both sides.
	rotated := options
	rotated.Keda.Password = "rotated-keda-pass"
	rotated.Domain.Password = "domain-pass-that-must-not-apply"
	script.runScript(t, rotated)
	golang.runGo(t, rotated)
	if !golang.canLogin(rotated.Keda) || !script.canLogin(rotated.Keda) {
		t.Error("the rotated KEDA password must work on both sides")
	}
	if golang.canLogin(rotated.Domain) || script.canLogin(rotated.Domain) {
		t.Error("an existing role's password must NOT be rewritten on either side")
	}
	if !golang.canLogin(options.Domain) || !script.canLogin(options.Domain) {
		t.Error("the original domain password must still work on both sides")
	}
}

func TestGoLegLeavesAnExistingRolesAttributesAloneLikeTheScript(t *testing.T) {
	t.Parallel()
	options := testOptions(false)
	script, golang := startSide(t), startSide(t)
	for _, s := range []*side{script, golang} {
		// An operator pre-created the domain role with extra privilege and another
		// password: neither provisioner touches it (the migrate preflight refuses it).
		if _, err := s.admin.Exec(context.Background(),
			`CREATE ROLE parity_domain LOGIN CREATEDB PASSWORD 'pre-existing'`); err != nil {
			t.Fatal(err)
		}
	}
	script.runScript(t, options)
	golang.runGo(t, options)
	diff(t, "pre-existing role", script.snapshot(t, options), golang.snapshot(t, options))
	problems, _, err := Verify(context.Background(), golang.admin, options)
	if err != nil || len(problems) == 0 {
		t.Fatalf("Verify must NAME the over-privileged pre-existing domain role, got %v %v", problems, err)
	}
}

func TestApplyIsAtomic(t *testing.T) {
	t.Parallel()
	golang := startSide(t)
	options := testOptions(true)
	options.RiverSchema = "no_such_river_schema" // the KEDA block, last, fails
	err := Apply(context.Background(), golang.admin, options)
	if err == nil {
		t.Fatal("a missing River schema must fail the KEDA grants")
	}
	if strings.Contains(err.Error(), options.Domain.Password) || strings.Contains(err.Error(), options.Keda.Password) {
		t.Fatalf("the error leaks a password: %v", err)
	}
	var roles int
	if err := golang.admin.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_roles WHERE rolname LIKE 'parity_%'`).Scan(&roles); err != nil {
		t.Fatal(err)
	}
	if roles != 0 {
		t.Fatalf("a failed Apply left %d role(s) behind: it must be all or nothing", roles)
	}
}

func TestApplyDoesNotGrantTablePrivilegesAndNeverDropsOwned(t *testing.T) {
	t.Parallel()
	golang := startSide(t)
	options := testOptions(false)
	golang.runGo(t, options)
	for _, role := range []string{options.Domain.Name, options.Queue.Name, options.Coordinator.Name} {
		var tableGrants int
		if err := golang.admin.QueryRow(context.Background(),
			`SELECT count(*) FROM information_schema.role_table_grants WHERE grantee = $1`, role).Scan(&tableGrants); err != nil {
			t.Fatal(err)
		}
		if tableGrants != 0 {
			t.Errorf("%s holds %d table grant(s) after provisioning: table privileges belong to `migrate river`", role, tableGrants)
		}
	}
}

// Verify names each broken bootstrap postcondition, and only that one. Its
// CONNECT and USAGE checks are EFFECTIVE privilege (PUBLIC counts), so those two
// cases break PUBLIC's grant as well as the role's.
func TestVerifyNamesEachBrokenPostconditionAndSeparatesThePublicCreateWarning(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	database, err := containers.DatabaseName(s.instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		break_  string
		restore string
		label   string
		detail  string
		warning bool
	}{
		"CONNECT revoked":        {"REVOKE CONNECT ON DATABASE " + database + " FROM parity_queue, PUBLIC", "GRANT CONNECT ON DATABASE " + database + " TO parity_queue, PUBLIC", "queue", "cannot CONNECT", false},
		"TEMPORARY granted":      {"GRANT TEMPORARY ON DATABASE " + database + " TO parity_coordinator", "REVOKE TEMPORARY ON DATABASE " + database + " FROM parity_coordinator", "coordinator", "holds TEMPORARY", false},
		"USAGE revoked":          {"REVOKE USAGE ON SCHEMA public FROM parity_domain, PUBLIC", "GRANT USAGE ON SCHEMA public TO parity_domain, PUBLIC", "domain", "lacks USAGE", false},
		"CREATE granted":         {"GRANT CREATE ON SCHEMA public TO parity_api", "REVOKE CREATE ON SCHEMA public FROM parity_api", "api", "holds CREATE on schema public in its own name", false},
		"CREATE only via PUBLIC": {"GRANT CREATE ON SCHEMA public TO PUBLIC", "REVOKE CREATE ON SCHEMA public FROM PUBLIC", "query_api", "only through PUBLIC", true},
		"KEDA cannot read":       {"REVOKE SELECT ON river.river_job FROM parity_keda", "GRANT SELECT ON river.river_job TO parity_keda", "keda", "cannot SELECT river_job", false},
		"role made superuser":    {"ALTER ROLE parity_queue SUPERUSER", "ALTER ROLE parity_queue NOSUPERUSER", "queue", "not an unprivileged login", false},
		"role made NOLOGIN":      {"ALTER ROLE parity_domain NOLOGIN", "ALTER ROLE parity_domain LOGIN", "domain", "not an unprivileged login", false},
		"role missing":           {"ALTER ROLE parity_keda RENAME TO parity_keda_gone", "ALTER ROLE parity_keda_gone RENAME TO parity_keda", "keda", "not an unprivileged login", false},
	} {
		if _, err := s.admin.Exec(ctx, test.break_); err != nil {
			t.Fatalf("%s: break: %v", name, err)
		}
		problems, warnings, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := problems
		if test.warning {
			got = warnings
			if len(problems) != 0 {
				t.Errorf("%s: a PUBLIC-inherited CREATE must be a warning, not a problem: %v", name, problems)
			}
		} else if len(warnings) != 0 {
			t.Errorf("%s: unexpected warnings %v", name, warnings)
		}
		matched := false
		for _, item := range got {
			if item.Role == test.label && strings.Contains(item.Detail, test.detail) {
				matched = true
			}
		}
		if !matched || len(got) < 1 {
			t.Errorf("%s: want %s %q in %v", name, test.label, test.detail, got)
		}
		if _, err := s.admin.Exec(ctx, test.restore); err != nil {
			t.Fatalf("%s: restore: %v", name, err)
		}
		if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
			t.Fatalf("%s: not clean after restore: %v %v %v", name, problems, warnings, err)
		}
	}
}

// r1 P1: a role name is used EXACTLY as configured. The script took --set values
// verbatim, so a name with leading or trailing spaces, quotes, upper case or
// non-ASCII characters is that exact role; the Go leg must provision the same one.
func TestGoLegMatchesTheScriptForUnusualRoleNames(t *testing.T) {
	t.Parallel()
	options := Options{
		Domain:      Role{` Edge Domain "quoted "`, "pw-1"},
		Queue:       Role{`Queue.With.Dots`, `p"w'\2`},
		Coordinator: Role{"Cördénator", "pw-3"},
	}
	script, golang := startSide(t), startSide(t)
	script.runScript(t, options)
	golang.runGo(t, options)
	diff(t, "unusual names", script.snapshot(t, options), golang.snapshot(t, options))
	for _, entry := range options.configured() {
		var exact bool
		if err := golang.admin.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, entry.role.Name).Scan(&exact); err != nil || !exact {
			t.Errorf("the role %q was not created under exactly that name: %v", entry.role.Name, err)
		}
	}
}
