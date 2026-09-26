//go:build integration

package roleprovision

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// startSide brings up one PostgreSQL with the River schema (KEDA reads river_job) and
// the REAL migrated application schema (KEDA reads public.sync_run_units, CHAOS-6946;
// the chart runs Alembic before provisioning, so the table exists there too).
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
	// Application schema first, River second: the order of the chart's hooks.
	pgschema.Apply(ctx, t, admin)
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

// runScript runs the psql script and then the ONE documented difference between it
// and the Go leg (CHAOS-6946): the KEDA login's SELECT on public.sync_run_units,
// which only `dho migrate roles` grants (the script is frozen and retires with the
// chart's psql Job). Applying it here keeps every parity test a parity test;
// runScriptOnly is the script alone, for the test that pins the difference.
func (s *side) runScript(t *testing.T, options Options) {
	t.Helper()
	s.runScriptOnly(t, options)
	if options.Keda.Name != "" {
		if _, err := s.admin.Exec(context.Background(), "GRANT SELECT ON public.sync_run_units TO "+ident(options.Keda.Name)); err != nil {
			t.Fatalf("the documented KEDA sync_run_units delta: %v", err)
		}
	}
}

func (s *side) runScriptOnly(t *testing.T, options Options) {
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

func TestGoLegRefusesAnOverPrivilegedPreExistingRoleWhereTheScriptLeavesItAlone(t *testing.T) {
	t.Parallel()
	options := testOptions(false)
	script, golang := startSide(t), startSide(t)
	for _, s := range []*side{script, golang} {
		// An operator pre-created the domain role with extra privilege and another
		// password. The script leaves it alone (and the migrate preflight refuses it
		// later); the Go leg REFUSES up front, before changing anything (r3 P1): a
		// deliberate, documented deviation.
		if _, err := s.admin.Exec(context.Background(),
			`CREATE ROLE parity_domain LOGIN CREATEDB PASSWORD 'pre-existing'`); err != nil {
			t.Fatal(err)
		}
	}
	script.runScript(t, options)
	err := Apply(context.Background(), golang.admin, options)
	if err == nil || !strings.Contains(err.Error(), "domain") || strings.Contains(err.Error(), "parity_domain") {
		t.Fatalf("Apply must refuse naming the label only: %v", err)
	}
	var created int
	if err := golang.admin.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_roles WHERE rolname IN ('parity_queue', 'parity_coordinator')`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("the refused Apply created %d role(s)", created)
	}
	var scriptCreated int
	if err := script.admin.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_roles WHERE rolname IN ('parity_queue', 'parity_coordinator')`).Scan(&scriptCreated); err != nil || scriptCreated != 2 {
		t.Fatalf("(documenting the deviation) the script provisions the other roles: %d %v", scriptCreated, err)
	}
	// Verify still NAMES the over-privileged role for a database the script touched.
	problems, _, err := Verify(context.Background(), script.admin, options)
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
		"CONNECT revoked":                 {"REVOKE CONNECT ON DATABASE " + database + " FROM parity_queue, PUBLIC", "GRANT CONNECT ON DATABASE " + database + " TO parity_queue, PUBLIC", "queue", "cannot CONNECT", false},
		"TEMPORARY granted":               {"GRANT TEMPORARY ON DATABASE " + database + " TO parity_coordinator", "REVOKE TEMPORARY ON DATABASE " + database + " FROM parity_coordinator", "coordinator", "holds TEMPORARY", false},
		"USAGE revoked":                   {"REVOKE USAGE ON SCHEMA public FROM parity_domain, PUBLIC", "GRANT USAGE ON SCHEMA public TO parity_domain, PUBLIC", "domain", "lacks USAGE", false},
		"CREATE granted":                  {"GRANT CREATE ON SCHEMA public TO parity_api", "REVOKE CREATE ON SCHEMA public FROM parity_api", "api", "holds CREATE on schema public in its own name", false},
		"CREATE only via PUBLIC":          {"GRANT CREATE ON SCHEMA public TO PUBLIC", "REVOKE CREATE ON SCHEMA public FROM PUBLIC", "query_api", "only through PUBLIC", true},
		"KEDA cannot read":                {"REVOKE SELECT ON river.river_job FROM parity_keda", "GRANT SELECT ON river.river_job TO parity_keda", "keda", "cannot SELECT river_job", false},
		"KEDA cannot read sync_run_units": {"REVOKE SELECT ON public.sync_run_units FROM parity_keda", "GRANT SELECT ON public.sync_run_units TO parity_keda", "keda", "cannot SELECT sync_run_units", false},
		"role made superuser":             {"ALTER ROLE parity_queue SUPERUSER", "ALTER ROLE parity_queue NOSUPERUSER", "queue", "not an unprivileged login", false},
		"role made NOLOGIN":               {"ALTER ROLE parity_domain NOLOGIN", "ALTER ROLE parity_domain LOGIN", "domain", "not an unprivileged login", false},
		"role missing":                    {"ALTER ROLE parity_keda RENAME TO parity_keda_gone", "ALTER ROLE parity_keda_gone RENAME TO parity_keda", "keda", "not an unprivileged login", false},
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

// A role name is used EXACTLY as configured. The runtime roles must be names `dho
// migrate river` accepts ([a-z_][a-z0-9_]*), so the interesting shapes there are a
// leading underscore, digits and the length limit; the KEDA login is never read by
// river and stays free-form (spaces, quotes, dots, upper case, non-ASCII), with a
// password full of quoting hazards. The Go leg must match the script on all of it.
func TestGoLegMatchesTheScriptForUnusualRoleNames(t *testing.T) {
	t.Parallel()
	options := Options{
		Domain:      Role{"_domain_9", "pw-1"},
		Queue:       Role{strings.Repeat("q", MaxIdentifierBytes), `p"w'\2`},
		Coordinator: Role{"c0", "pw\n3"},
		Keda:        Role{` Edge Keda "quoted "É.Dots`, "k'e\"d\\a"},
		RiverSchema: "river",
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

// r1b P3: a PRE-EXISTING role can hold grants the script's REVOKEs remove (an explicit
// TEMPORARY on the database, an explicit CREATE on schema public). The Go leg must
// remove exactly the same ones, for every role including KEDA's TEMPORARY.
func TestGoLegRevokesTheSameStrayGrantsAsTheScript(t *testing.T) {
	t.Parallel()
	options := testOptions(true)
	script, golang := startSide(t), startSide(t)
	for _, s := range []*side{script, golang} {
		database, err := containers.DatabaseName(s.instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range options.configured() {
			for _, statement := range []string{
				"CREATE ROLE " + ident(entry.role.Name) + " LOGIN PASSWORD 'stray-" + entry.label + "'",
				"GRANT TEMPORARY ON DATABASE " + ident(database) + " TO " + ident(entry.role.Name),
				"GRANT CREATE ON SCHEMA public TO " + ident(entry.role.Name),
			} {
				if _, err := s.admin.Exec(context.Background(), statement); err != nil {
					t.Fatalf("%s: %v", entry.label, err)
				}
			}
		}
	}
	script.runScript(t, options)
	golang.runGo(t, options)
	scriptSnapshot, goSnapshot := script.snapshot(t, options), golang.snapshot(t, options)
	diff(t, "pre-existing roles with stray grants", scriptSnapshot, goSnapshot)
	joined := strings.Join(goSnapshot, "\n")
	if strings.Contains(joined, "keda grant TEMPORARY") {
		t.Errorf("the KEDA role's stray TEMPORARY must be revoked:\n%s", joined)
	}
}

// r1b P1-1: Verify must not be green for a runtime role the readiness check will
// refuse for owning an object or being a member of another role.
func TestVerifyRefusesRuntimeRolesThatOwnObjectsOrHaveMemberships(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	for name, test := range map[string]struct {
		break_  []string
		restore []string
		label   string
		detail  string
	}{
		"owns a schema": {
			[]string{"CREATE SCHEMA owned_by_domain AUTHORIZATION parity_domain"},
			[]string{"DROP SCHEMA owned_by_domain"}, "domain", "owns"},
		"member of another role": {
			[]string{"CREATE ROLE some_group NOLOGIN", "GRANT some_group TO parity_queue"},
			[]string{"REVOKE some_group FROM parity_queue", "DROP ROLE some_group"}, "queue", "member of another role"},
		"query-api owns a schema": {
			[]string{"CREATE SCHEMA owned_by_qapi AUTHORIZATION parity_query_api"},
			[]string{"DROP SCHEMA owned_by_qapi"}, "query_api", "owns"},
	} {
		for _, statement := range test.break_ {
			if _, err := s.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		problems, _, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for _, item := range problems {
			if item.Role == test.label && strings.Contains(item.Detail, test.detail) {
				matched = true
			}
		}
		if !matched {
			t.Errorf("%s: want %s %q in %v", name, test.label, test.detail, problems)
		}
		for _, statement := range test.restore {
			if _, err := s.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s restore: %v", name, err)
			}
		}
		if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
			t.Fatalf("%s: not clean after restore: %v %v %v", name, problems, warnings, err)
		}
	}
}

// r1b P1-2: the KEDA login is read-only on river_job and holds nothing else.
func TestVerifyRefusesAKedaRoleThatHoldsAnythingBeyondItsReadOnlyGrants(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	database, err := containers.DatabaseName(s.instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct{ break_, restore string }{
		"UPDATE on river_job":        {"GRANT UPDATE ON river.river_job TO parity_keda", "REVOKE UPDATE ON river.river_job FROM parity_keda"},
		"SELECT on another table":    {"GRANT SELECT ON river.river_leader TO parity_keda", "REVOKE SELECT ON river.river_leader FROM parity_keda"},
		"CREATE on the river schema": {"GRANT CREATE ON SCHEMA river TO parity_keda", "REVOKE CREATE ON SCHEMA river FROM parity_keda"},
		// CHAOS-6946: SELECT on public.sync_run_units is the ONLY grant beyond river_job.
		"UPDATE on sync_run_units":           {"GRANT UPDATE ON public.sync_run_units TO parity_keda", "REVOKE UPDATE ON public.sync_run_units FROM parity_keda"},
		"INSERT on sync_run_units":           {"GRANT INSERT ON public.sync_run_units TO parity_keda", "REVOKE INSERT ON public.sync_run_units FROM parity_keda"},
		"a grant option on sync_run_units":   {"GRANT SELECT ON public.sync_run_units TO parity_keda WITH GRANT OPTION", "REVOKE GRANT OPTION FOR SELECT ON public.sync_run_units FROM parity_keda"},
		"a column UPDATE on sync_run_units":  {"GRANT UPDATE (status) ON public.sync_run_units TO parity_keda", "REVOKE UPDATE (status) ON public.sync_run_units FROM parity_keda"},
		"SELECT on another public table":     {"GRANT SELECT ON public.sync_runs TO parity_keda", "REVOKE SELECT ON public.sync_runs FROM parity_keda"},
		"SELECT on a sync_run_units sibling": {"GRANT SELECT ON public.sync_run_unit_effect_chunks TO parity_keda", "REVOKE SELECT ON public.sync_run_unit_effect_chunks FROM parity_keda"},
		"CREATE on the database":             {"GRANT CREATE ON DATABASE " + database + " TO parity_keda", "REVOKE CREATE ON DATABASE " + database + " FROM parity_keda"},
	} {
		if _, err := s.admin.Exec(ctx, test.break_); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		problems, _, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for _, item := range problems {
			if item.Role == "keda" && strings.Contains(item.Detail, "unexpected privilege") {
				matched = true
			}
		}
		if !matched {
			t.Errorf("%s: a KEDA role holding more than read-only river_job must be a problem: %v", name, problems)
		}
		if _, err := s.admin.Exec(ctx, test.restore); err != nil {
			t.Fatalf("%s restore: %v", name, err)
		}
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("not clean after restore: %v %v %v", problems, warnings, err)
	}
}

// Lead D2616 shape rule: for the runtime roles Verify reads the SAME enumeration and
// closure self-check the readiness checks use. It reports what neither `migrate roles`
// nor `migrate river` grants (database/schema privileges beyond CONNECT and USAGE,
// classes river never touches, another database's ACL) and leaves river's own kinds
// (relations, columns, functions, default privileges, schema USAGE) to that role's
// readiness check, which the end-to-end test runs.
func TestVerifyRuntimeRolesHoldOnlyWhatRolesAndRiverGrantOfAnyCatalogClass(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	database, err := containers.DatabaseName(s.instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	// river's own kind of grant is NOT a problem.
	if _, err := s.admin.Exec(ctx, "GRANT SELECT, INSERT ON river.river_job TO parity_domain"); err != nil {
		t.Fatal(err)
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("a relation grant (migrate river's) must not be a problem: %v %v %v", problems, warnings, err)
	}
	if _, err := s.admin.Exec(ctx, "REVOKE SELECT, INSERT ON river.river_job FROM parity_domain"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.admin.Exec(ctx, "CREATE DATABASE other_db"); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		break_, restore, label string
		wantProblem            bool
	}{
		"CREATE on the database":                                        {"GRANT CREATE ON DATABASE " + database + " TO parity_queue", "REVOKE CREATE ON DATABASE " + database + " FROM parity_queue", "queue", true},
		"CREATE on the river schema":                                    {"GRANT CREATE ON SCHEMA river TO parity_coordinator", "REVOKE CREATE ON SCHEMA river FROM parity_coordinator", "coordinator", true},
		"USAGE on the river schema (river's own kind)":                  {"GRANT USAGE ON SCHEMA river TO parity_domain", "REVOKE USAGE ON SCHEMA river FROM parity_domain", "domain", false},
		"EXECUTE on a function (river's own kind, judged by readiness)": {"CREATE FUNCTION public.roleprovision_probe() RETURNS int LANGUAGE sql AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION public.roleprovision_probe() TO parity_api", "DROP FUNCTION public.roleprovision_probe()", "api", false},
		"USAGE on a language":                                           {"GRANT USAGE ON LANGUAGE sql TO parity_coordinator", "REVOKE USAGE ON LANGUAGE sql FROM parity_coordinator", "coordinator", true},
		"a large object":                                                {"SELECT lo_create(424242); GRANT SELECT ON LARGE OBJECT 424242 TO parity_queue", "REVOKE SELECT ON LARGE OBJECT 424242 FROM parity_queue; SELECT lo_unlink(424242)", "queue", true},
		"a foreign data wrapper":                                        {"CREATE FOREIGN DATA WRAPPER probe_fdw; GRANT USAGE ON FOREIGN DATA WRAPPER probe_fdw TO parity_domain", "DROP FOREIGN DATA WRAPPER probe_fdw", "domain", true},
		"a foreign server":                                              {"CREATE FOREIGN DATA WRAPPER probe_fdw2; CREATE SERVER probe_server FOREIGN DATA WRAPPER probe_fdw2; GRANT USAGE ON FOREIGN SERVER probe_server TO parity_api", "DROP SERVER probe_server; DROP FOREIGN DATA WRAPPER probe_fdw2", "api", true},
		"a configuration parameter":                                     {"GRANT SET ON PARAMETER work_mem TO parity_query_api", "REVOKE SET ON PARAMETER work_mem FROM parity_query_api", "query_api", true},
		"CONNECT on another database":                                   {"GRANT CONNECT ON DATABASE other_db TO parity_query_api", "REVOKE CONNECT ON DATABASE other_db FROM parity_query_api", "query_api", true},
		"a grant option on a relation":                                  {"GRANT SELECT ON river.river_job TO parity_domain WITH GRANT OPTION", "REVOKE SELECT ON river.river_job FROM parity_domain", "domain", false},
	} {
		if _, err := s.admin.Exec(ctx, test.break_); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		problems, _, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		reported := false
		for _, item := range problems {
			if item.Role == test.label && strings.Contains(item.Detail, "unexpected privilege") {
				reported = true
			}
		}
		// A grant option on a relation is a relation-class grant, which river owns:
		// the exactness of relation grants is that role's readiness check, not this one.
		if reported != test.wantProblem {
			t.Errorf("%s: reported=%v, want %v for %s: %v", name, reported, test.wantProblem, test.label, problems)
		}
		if _, err := s.admin.Exec(ctx, test.restore); err != nil {
			t.Fatalf("%s restore: %v", name, err)
		}
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("not clean after restore: %v %v %v", problems, warnings, err)
	}
}

// r2 P1-a: the KEDA login is read-only in EFFECT: a write it holds through PUBLIC or
// through a role membership is as bad as one granted to it directly.
func TestVerifyRefusesAKedaRoleThatCanWriteThroughPublicOrAMembership(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	for name, test := range map[string]struct {
		break_  []string
		restore []string
		detail  string
	}{
		"UPDATE through PUBLIC": {
			[]string{"GRANT UPDATE ON river.river_job TO PUBLIC"},
			[]string{"REVOKE UPDATE ON river.river_job FROM PUBLIC"}, "unexpected privilege"},
		"UPDATE on sync_run_units through PUBLIC": {
			[]string{"GRANT UPDATE ON public.sync_run_units TO PUBLIC"},
			[]string{"REVOKE UPDATE ON public.sync_run_units FROM PUBLIC"}, "unexpected privilege"},
		"a membership that carries UPDATE": {
			[]string{"CREATE ROLE writers NOLOGIN", "GRANT UPDATE ON river.river_job TO writers", "GRANT writers TO parity_keda"},
			[]string{"REVOKE writers FROM parity_keda", "REVOKE UPDATE ON river.river_job FROM writers", "DROP ROLE writers"}, "member of another role"},
	} {
		for _, statement := range test.break_ {
			if _, err := s.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		problems, _, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for _, item := range problems {
			if item.Role == "keda" && strings.Contains(item.Detail, test.detail) {
				matched = true
			}
		}
		if !matched {
			t.Errorf("%s: the KEDA role can write but Verify is green: %v", name, problems)
		}
		for _, statement := range test.restore {
			if _, err := s.admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s restore: %v", name, err)
			}
		}
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("not clean after restore: %v %v %v", problems, warnings, err)
	}
}

// r2 P1-c at the library seam: an Authenticate closure classifies a password
// mismatch (28P01) as a problem and any other failure as a warning; nothing changes.
func TestVerifyAuthenticatesTheSuppliedPasswords(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	options.Authenticate = func(ctx context.Context, role Role) error {
		switch role.Name {
		case "parity_domain":
			return &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
		case "parity_queue":
			return &pgconn.PgError{Code: "28000", Message: "no pg_hba.conf entry"}
		}
		return nil
	}
	problems, warnings, err := Verify(ctx, s.admin, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || problems[0].Role != "domain" || !strings.Contains(problems[0].Detail, "does not authenticate") {
		t.Errorf("a 28P01 must be a problem naming the domain label: %v", problems)
	}
	if len(warnings) != 1 || warnings[0].Role != "queue" || !strings.Contains(warnings[0].Detail, "could not verify") {
		t.Errorf("any other failure must be a warning naming the queue label: %v", warnings)
	}
	for _, item := range append(append([]Problem{}, problems...), warnings...) {
		if strings.Contains(item.String(), "parity_") || strings.Contains(item.String(), "pw") {
			t.Errorf("a role name or password leaked into %q", item.String())
		}
	}
}

// recordingQuerier is a roleacl.Querier that records, per statement, the role it was
// asked about (the first argument).
type recordingQuerier struct {
	inner roleacl.Querier
	mu    sync.Mutex
	calls []recordedCall
}

type recordedCall struct{ sql, role string }

func (r *recordingQuerier) record(sql string, args []any) {
	role := ""
	if len(args) > 0 {
		if text, ok := args[0].(string); ok {
			role = text
		}
	}
	r.mu.Lock()
	r.calls = append(r.calls, recordedCall{sql, role})
	r.mu.Unlock()
}

func (r *recordingQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.record(sql, args)
	return r.inner.Query(ctx, sql, args...)
}

func (r *recordingQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	r.record(sql, args)
	return r.inner.QueryRow(ctx, sql, args...)
}

func (r *recordingQuerier) ran(fragment, role string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.calls {
		if call.role == role && strings.Contains(call.sql, fragment) {
			return true
		}
	}
	return false
}

// Lead D2616 structural condition: Verify runs the roleacl closure (identity attributes,
// member-of-no-role, owns-nothing, and the effective-grant enumeration with its
// ACL-dependency self-check) for EVERY configured role, KEDA included, so a
// role-specific shortcut around it fails here. The manifest roles are ENUMERATED from
// the options, not listed by hand.
func TestVerifyRunsTheRoleaclClosureForEveryConfiguredRole(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	recorder := &recordingQuerier{inner: s.admin}
	if problems, warnings, err := Verify(context.Background(), recorder, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("Verify: %v %v %v", problems, warnings, err)
	}
	configured := options.configured()
	if len(configured) != 6 {
		t.Fatalf("the manifest must enumerate all six roles, got %d", len(configured))
	}
	closure := map[string]string{
		"identity attributes":         roleacl.RoleAttributesSQL,
		"membership-free":             roleacl.MembershipFreeSQL,
		"owns nothing":                roleacl.OwnsNothingSQL,
		"effective-grant enumeration": "'relation' AS class",
	}
	for _, entry := range configured {
		for name, fragment := range closure {
			if !recorder.ran(fragment, entry.role.Name) {
				t.Errorf("Verify did not run the roleacl %s check for the %s role", name, entry.label)
			}
		}
	}
}

// Every configured role, enumerated from the options, is refused when it is a member
// of another role or owns an object: the verdict, not just the query, applies to all.
func TestVerifyReportsMembershipAndOwnershipForEveryConfiguredRole(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	exec := func(statement string) {
		t.Helper()
		if _, err := s.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, entry := range options.configured() {
		role := ident(entry.role.Name)
		exec("CREATE ROLE every_group NOLOGIN")
		exec("GRANT every_group TO " + role)
		exec("CREATE SCHEMA every_owned AUTHORIZATION " + role)
		problems, _, err := Verify(ctx, s.admin, options)
		if err != nil {
			t.Fatal(err)
		}
		var member, owns bool
		for _, item := range problems {
			if item.Role != entry.label {
				continue
			}
			member = member || strings.Contains(item.Detail, "member of another role")
			owns = owns || strings.Contains(item.Detail, "owns an object")
		}
		if !member || !owns {
			t.Errorf("%s: member=%v owns=%v, both must be reported: %v", entry.label, member, owns, problems)
		}
		exec("DROP SCHEMA every_owned")
		exec("REVOKE every_group FROM " + role)
		exec("DROP ROLE every_group")
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("not clean after restore: %v %v %v", problems, warnings, err)
	}
}

// r3 P1 (lead D2648): a PRE-EXISTING role that is not an eligible unprivileged login is
// refused BEFORE any statement runs: nothing is created, nothing is granted, and a
// member of an existing NOLOGIN group named like the KEDA login gains no access.
func TestApplyRefusesAnIneligiblePreExistingRoleBeforeChangingAnything(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE ROLE " + ident(options.Keda.Name) + " NOLOGIN",
		"CREATE ROLE keda_member LOGIN PASSWORD 'member-pw'",
		"GRANT " + ident(options.Keda.Name) + " TO keda_member",
	} {
		if _, err := s.admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	err := Apply(ctx, s.admin, options)
	if !errors.Is(err, ErrInvalidOptions) && !errors.Is(err, ErrProvisioning) {
		t.Fatalf("an ineligible pre-existing role must be refused, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "keda") || strings.Contains(err.Error(), options.Keda.Name) {
		t.Fatalf("the refusal must name the LABEL and never the role name: %v", err)
	}
	var created int
	if err := s.admin.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname LIKE 'parity_%' AND rolname <> $1`, options.Keda.Name).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("the refused Apply created %d role(s): it must change nothing", created)
	}
	var reads bool
	if err := s.admin.QueryRow(ctx, `SELECT has_table_privilege('keda_member', 'river.river_job', 'SELECT')`).Scan(&reads); err != nil {
		t.Fatal(err)
	}
	if reads {
		t.Fatal("a member of the existing NOLOGIN group gained SELECT on river_job")
	}
}

// r3 P2 (lead D2648): a privilege a runtime role holds through PUBLIC is judged like the
// KEDA login's: ambient CONNECT and USAGE on public and PUBLIC's CREATE on public (a
// warning) are fine; anything else, INCLUDING a relation or column privilege (PUBLIC is
// never in any posture), is a problem for every configured role.
func TestVerifyReportsAnUnexpectedPublicGrantForEveryConfiguredRole(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	options := testOptions(true)
	s.runGo(t, options)
	ctx := context.Background()
	for _, statement := range []string{"GRANT USAGE ON SCHEMA river TO PUBLIC", "GRANT UPDATE ON river.river_job TO PUBLIC"} {
		if _, err := s.admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	problems, _, err := Verify(ctx, s.admin, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range options.configured() {
		found := false
		for _, item := range problems {
			if item.Role == entry.label && strings.Contains(item.Detail, "unexpected privilege") && strings.Contains(item.Detail, "through PUBLIC") {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: an UPDATE on river_job through PUBLIC must be a problem: %v", entry.label, problems)
		}
	}
}

// CHAOS-6946: the KEDA login reads public.sync_run_units (the go-sync ScaledObject's
// planned-backlog trigger), granted by `dho migrate roles` and by nothing else. The
// psql script (frozen) never granted it, so this is the deliberate difference the
// parity harness applies by hand (runScript). The state the grant exists to reach is
// the LOGIN running the trigger's query, not a catalog row.
func TestGoLegGrantsTheKedaLoginSelectOnSyncRunUnitsAndNothingMore(t *testing.T) {
	t.Parallel()
	options := testOptions(true)
	script, golang := startSide(t), startSide(t)
	script.runScriptOnly(t, options)
	golang.runGo(t, options)
	const triggerQuery = `SELECT count(*) FROM public.sync_run_units WHERE status = 'planned' AND (available_at IS NULL OR available_at <= now())`
	kedaRun := func(s *side, query string) error {
		t.Helper()
		config, err := pgx.ParseConfig(s.instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.User, config.Password = options.Keda.Name, options.Keda.Password
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		connection, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Fatalf("the KEDA login cannot connect: %v", err)
		}
		defer connection.Close(ctx)
		var ignored any
		return connection.QueryRow(ctx, query).Scan(&ignored)
	}
	// The baseline: the script alone leaves the login unable to run the trigger query.
	var denied *pgconn.PgError
	if err := kedaRun(script, triggerQuery); !errors.As(err, &denied) || denied.Code != "42501" {
		t.Fatalf("(baseline) the psql script alone must leave the KEDA login without SELECT on sync_run_units: %v", err)
	}
	if err := kedaRun(golang, triggerQuery); err != nil {
		t.Fatalf("after `migrate roles` the KEDA login must run the go-sync trigger query: %v", err)
	}
	// Nothing more: no other privilege on the table, no other public table.
	for _, query := range []string{
		`SELECT count(*) FROM public.sync_runs`,
		`SELECT count(*) FROM public.sync_run_unit_effect_chunks`,
		`SELECT count(*) FROM public.integrations`,
	} {
		if err := kedaRun(golang, query); !errors.As(err, &denied) || denied.Code != "42501" {
			t.Errorf("the KEDA login must not read anything else (%s): %v", query, err)
		}
	}
	var writes bool
	if err := golang.admin.QueryRow(context.Background(), `SELECT has_table_privilege($1, 'public.sync_run_units', 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
		OR has_any_column_privilege($1, 'public.sync_run_units', 'INSERT, UPDATE, REFERENCES')`, options.Keda.Name).Scan(&writes); err != nil || writes {
		t.Fatalf("the KEDA login holds a non-SELECT privilege on sync_run_units: %v %v", writes, err)
	}
	// Idempotent, and Verify is green (the closure names the new privilege as EXPECTED).
	golang.runGo(t, options)
	if problems, warnings, err := Verify(context.Background(), golang.admin, options); err != nil || len(problems)+len(warnings) != 0 {
		t.Fatalf("Verify: %v %v %v", problems, warnings, err)
	}
	if !strings.Contains(strings.Join(golang.snapshot(t, options), "\n"), "keda grant SELECT on relation public.sync_run_units") {
		t.Errorf("the enumeration must list the KEDA login's sync_run_units SELECT:\n%s", strings.Join(golang.snapshot(t, options), "\n"))
	}
}

// The table is Alembic's, created before provisioning in the chart and in Compose. If
// it is missing, Apply must fail (all or nothing, label only) and Verify must NAME the
// missing read, never return an error or pass.
func TestKedaSyncRunUnitsMissingFailsApplyAtomicallyAndVerifyNamesIt(t *testing.T) {
	t.Parallel()
	golang := startSide(t)
	options := testOptions(true)
	golang.runGo(t, options)
	if _, err := golang.admin.Exec(context.Background(), "DROP TABLE public.sync_run_units CASCADE"); err != nil {
		t.Fatal(err)
	}
	problems, _, err := Verify(context.Background(), golang.admin, options)
	if err != nil {
		t.Fatalf("Verify must report a missing table as a problem, not an error: %v", err)
	}
	named := false
	for _, item := range problems {
		if item.Role == "keda" && strings.Contains(item.Detail, "cannot SELECT sync_run_units") {
			named = true
		}
	}
	if !named {
		t.Errorf("Verify must name the KEDA login's missing sync_run_units read: %v", problems)
	}
	fresh := startSide(t)
	if _, err := fresh.admin.Exec(context.Background(), "DROP TABLE public.sync_run_units CASCADE"); err != nil {
		t.Fatal(err)
	}
	err = Apply(context.Background(), fresh.admin, options)
	if err == nil || !strings.Contains(err.Error(), "sync_run_units") || strings.Contains(err.Error(), options.Keda.Name) || strings.Contains(err.Error(), options.Keda.Password) {
		t.Fatalf("Apply without the table must fail naming the step, never the role or password: %v", err)
	}
	var roles int
	if err := fresh.admin.QueryRow(context.Background(), `SELECT count(*) FROM pg_roles WHERE rolname LIKE 'parity_%'`).Scan(&roles); err != nil || roles != 0 {
		t.Fatalf("a failed Apply left %d role(s) behind: %v", roles, err)
	}
}
