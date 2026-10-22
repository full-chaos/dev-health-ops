//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// publicReadOracle is the reviewed manifest, written out independently of
// PublicReadColumns (a unit test asserts the two are equal). Every table gets
// one extra column the runtime role must NOT be able to read.
var publicReadOracle = map[string][]string{
	"users": {"id", "email", "password_hash", "full_name", "username", "auth_provider", "auth_provider_id",
		"is_active", "is_verified", "is_superuser", "token_version", "last_login_at"},
	"memberships": {"user_id", "org_id", "role", "joined_at", "created_at"},
	"sso_providers": {"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
		"config", "allowed_domains", "encrypted_secrets", "is_default", "created_at"},
	"organizations":                {"id", "slug", "name", "is_active", "tier"},
	"saml_assertion_replays":       {"provider_id", "assertion_id", "expires_at"},
	"org_licenses":                 {"org_id", "tier", "features_override"},
	"internal_service_credentials": {"id", "service_name", "token_hash", "scopes", "expires_at", "revoked_at"},
}

type publicReadsEnv struct {
	*authFixture
	logs *bytes.Buffer
}

func (e *publicReadsEnv) options(logs *bytes.Buffer) Options {
	return Options{Schema: e.schema, RuntimeRole: e.runtimeRole, Logger: slog.New(slog.NewTextHandler(logs, nil))}
}

// newPublicReadsFixture loads the REAL ops schema (the pgmigrate baseline and
// chain), hands the manifest tables to the migration login (as the real ones
// belong to whoever holds GRANT OPTION), moves the tables named in hide out of
// the way, then re-runs Apply so the grants land.
func newPublicReadsFixture(t *testing.T, ctx context.Context, hide ...string) *publicReadsEnv {
	t.Helper()
	env := newM10FixtureWith(t, ctx, func(ctx context.Context, instance *containers.Instance, migrationRole string) {
		conn, err := pgx.Connect(ctx, instance.URI)
		if err != nil {
			t.Fatalf("connect for the ops schema: %v", err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		baseline, err := pgmigrate.LoadBaseline()
		if err != nil {
			t.Fatal(err)
		}
		chain, err := pgmigrate.LoadChain()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
			t.Fatalf("apply the ops schema: %v", err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(`GRANT CREATE ON SCHEMA public TO %s`, Quote(mustValidatedIdentifier(migrationRole)))); err != nil {
			t.Fatal(err)
		}
		owned := []string{LegacyRefreshTokenGrant().Table}
		for table := range publicReadOracle {
			owned = append(owned, table)
		}
		for _, table := range owned {
			if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.%s OWNER TO %s`,
				Quote(mustValidatedIdentifier(table)), Quote(mustValidatedIdentifier(migrationRole)))); err != nil {
				t.Fatalf("hand public.%s to the migration login: %v", table, err)
			}
		}
		for _, table := range hide {
			if _, err := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.%s RENAME TO %s`,
				Quote(mustValidatedIdentifier(table)), Quote(mustValidatedIdentifier(table+"_hidden")))); err != nil {
				t.Fatalf("hide public.%s: %v", table, err)
			}
		}
	})
	e := &publicReadsEnv{authFixture: env}
	e.logs = &bytes.Buffer{}
	if _, err := Apply(ctx, env.migration, e.options(e.logs)); err != nil {
		t.Fatalf("Apply over the real ops schema: %v", err)
	}
	return e
}

func (e *publicReadsEnv) rename(t *testing.T, ctx context.Context, from, to string) {
	t.Helper()
	if _, err := e.migration.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.%s RENAME TO %s`,
		Quote(mustValidatedIdentifier(from)), Quote(mustValidatedIdentifier(to)))); err != nil {
		t.Fatalf("rename public.%s: %v", from, err)
	}
}

// extraColumn returns a real column of public.<table> that the manifest does
// not list.
func (e *publicReadsEnv) extraColumn(t *testing.T, ctx context.Context, table string) string {
	t.Helper()
	var column string
	if err := e.admin.QueryRow(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name <> ALL($2::text[])
		ORDER BY ordinal_position LIMIT 1`, table, publicReadOracle[table]).Scan(&column); err != nil {
		t.Fatalf("a column of public.%s outside the manifest: %v", table, err)
	}
	return column
}

func (e *publicReadsEnv) violations(t *testing.T, ctx context.Context) []Violation {
	t.Helper()
	conn, err := e.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), e.options(&bytes.Buffer{}))
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	return found
}

// runtimeStatement runs one statement as the runtime role and reports the
// error pgx surfaces only once the rows are read.
func (e *publicReadsEnv) runtimeStatement(ctx context.Context, statement string) error {
	rows, err := e.runtime.Query(ctx, statement)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close()
	return rows.Err()
}

func describe(found []Violation) string {
	lines := make([]string, 0, len(found))
	for _, v := range found {
		lines = append(lines, v.String())
	}
	return strings.Join(lines, "\n")
}

func TestPublicReadsRuntimeReadsExactlyTheManifestColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)

	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after Apply is not clean:\n%s", describe(found))
	}
	for table, columns := range publicReadOracle {
		quoted := make([]string, len(columns))
		for i, column := range columns {
			quoted[i] = Quote(mustValidatedIdentifier(column))
		}
		query := fmt.Sprintf(`SELECT %s FROM public.%s`, strings.Join(quoted, ", "), Quote(mustValidatedIdentifier(table)))
		if err := env.runtimeStatement(ctx, query); err != nil {
			t.Errorf("runtime cannot read the manifest columns of %s: %v", table, err)
		}
		extra := Quote(mustValidatedIdentifier(env.extraColumn(t, ctx, table)))
		first := Quote(mustValidatedIdentifier(columns[0]))
		quotedTable := Quote(mustValidatedIdentifier(table))
		for name, statement := range map[string]string{
			"another column": fmt.Sprintf(`SELECT %s FROM public.%s`, extra, quotedTable),
			"every column":   fmt.Sprintf(`SELECT * FROM public.%s`, quotedTable),
			"an insert":      fmt.Sprintf(`INSERT INTO public.%s DEFAULT VALUES`, quotedTable),
			"an update":      fmt.Sprintf(`UPDATE public.%s SET %s = %s`, quotedTable, first, first),
			"a delete":       fmt.Sprintf(`DELETE FROM public.%s`, quotedTable),
		} {
			if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s on public.%s: err = %v, want permission denied for the runtime role", name, table, err)
			}
		}
	}
	// A public table outside the manifest stays unreachable.
	if _, err := env.migration.Exec(ctx, `CREATE TABLE public.ops_unrelated (id text)`); err != nil {
		t.Fatal(err)
	}
	if err := env.runtimeStatement(ctx, `SELECT id FROM public.ops_unrelated`); err == nil {
		t.Error("the runtime role can read a public table outside the manifest")
	}
}

func TestPublicReadsMissingColumnGrantFailsOnATableThatExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)

	if _, err := env.migration.Exec(ctx, fmt.Sprintf(`REVOKE SELECT (email) ON public.users FROM %s`, Quote(mustValidatedIdentifier(env.runtimeRole)))); err != nil {
		t.Fatal(err)
	}
	found := env.violations(t, ctx)
	if !strings.Contains(describe(found), "users") || !strings.Contains(describe(found), "email") {
		t.Fatalf("a missing column grant on an existing table was not reported:\n%s", describe(found))
	}
	// Apply repairs it, and a failing verify makes Apply itself fail first.
	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err != nil {
		t.Fatalf("Apply did not repair the missing grant: %v", err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after repair is not clean:\n%s", describe(found))
	}
}

func TestPublicReadsExtraColumnPrivilegesAreViolationsAndNarrowingNeedsAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.runtimeRole))
	usersExtra := Quote(mustValidatedIdentifier(env.extraColumn(t, ctx, "users")))

	for name, statement := range map[string]string{
		"SELECT on a column outside the manifest": `GRANT SELECT (` + usersExtra + `) ON public.users TO ` + role,
		"UPDATE on a manifest column":             `GRANT UPDATE (email) ON public.users TO ` + role,
		"SELECT on an unrelated public table":     `CREATE TABLE public.ops_unrelated (id text); GRANT SELECT (id) ON public.ops_unrelated TO ` + role,
	} {
		if _, err := env.migration.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if found := env.violations(t, ctx); len(found) == 0 {
			t.Errorf("%s was not reported", name)
		}
	}

	// Removing a privilege the role holds is a narrowing: refused, and nothing
	// changes, until the exact set is acknowledged.
	_, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{}))
	var narrowing *NarrowingError
	if !errors.As(err, &narrowing) {
		t.Fatalf("Apply = %v, want a *NarrowingError for the extra public column privileges", err)
	}
	if !strings.Contains(narrowing.Error(), "users") {
		t.Errorf("the refusal does not name the table: %v", narrowing)
	}
	var stillHeld bool
	if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', 'email', 'UPDATE')`, env.runtimeRole).Scan(&stillHeld); err != nil || !stillHeld {
		t.Errorf("a refused run changed the grants (UPDATE on users.email held = %v, %v)", stillHeld, err)
	}

	// An unrelated public table is not on the manifest: Apply leaves it, and the
	// posture keeps failing until an operator revokes it.
	acknowledged := env.options(&bytes.Buffer{})
	acknowledged.AcknowledgeNarrowing = narrowing.Digest
	if _, err := Apply(ctx, env.migration, acknowledged); err == nil {
		t.Error("Apply succeeded while the runtime role held a column privilege on an unrelated public table")
	}
	if _, err := env.migration.Exec(ctx, `REVOKE ALL ON public.ops_unrelated FROM `+role); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
		t.Fatalf("Apply with the acknowledged narrowing: %v", err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after the acknowledged narrowing is not clean:\n%s", describe(found))
	}
}

func TestPublicReadsAbsentTableIsSkippedAndLoggedByName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx, "org_licenses")

	logs := &bytes.Buffer{}
	if _, err := Apply(ctx, env.migration, env.options(logs)); err != nil {
		t.Fatalf("Apply with an absent manifest table: %v", err)
	}
	if !strings.Contains(logs.String(), "org_licenses") {
		t.Errorf("the skipped table was not logged by name:\n%s", logs.String())
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("an absent table must not fail the posture:\n%s", describe(found))
	}
	// Once the table exists, the grant is required.
	env.rename(t, ctx, "org_licenses_hidden", "org_licenses")
	if found := env.violations(t, ctx); !strings.Contains(describe(found), "org_licenses") {
		t.Fatalf("a table that exists without its grant was not reported:\n%s", describe(found))
	}
}

// A view named like a manifest table is not that table: the grant path must
// not skip it, and a column privilege on it must not pass as the manifest's.
func TestPublicReadsSameNamedViewIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx, "users")
	role := Quote(mustValidatedIdentifier(env.runtimeRole))

	if _, err := env.migration.Exec(ctx, `CREATE VIEW public.users AS SELECT 'view-secret'::text AS id`); err != nil {
		t.Fatal(err)
	}
	if _, err := env.migration.Exec(ctx, `GRANT SELECT (id) ON public.users TO `+role); err != nil {
		t.Fatal(err)
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), env.options(&bytes.Buffer{}))
	if err == nil && !strings.Contains(describe(found), "users") {
		t.Errorf("a column privilege on a same-named view passed the posture:\n%s", describe(found))
	}
	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err == nil {
		t.Error("Apply succeeded with a view standing in for a manifest table")
	}
}

// MAINTAIN (PostgreSQL 17+) is a table-level privilege like the others: the
// posture must see it on the ops tables and on the auth schema alike.
func TestPostureReportsTheMaintainPrivilege(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.runtimeRole))

	for name, statement := range map[string]string{
		"an ops table":  `GRANT MAINTAIN ON public.users TO ` + role,
		"an auth table": `GRANT MAINTAIN ON auth.sessions TO ` + role,
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		found := env.violations(t, ctx)
		if !strings.Contains(describe(found), "MAINTAIN") {
			t.Errorf("MAINTAIN on %s was not reported:\n%s", name, describe(found))
		}
		if _, err := env.admin.Exec(ctx, strings.Replace(strings.Replace(statement, "GRANT", "REVOKE", 1), " TO ", " FROM ", 1)); err != nil {
			t.Fatal(err)
		}
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after revoking is not clean:\n%s", describe(found))
	}
}

// A role the runtime can SET ROLE to without inheriting it is invisible to the
// effective-privilege checks, so reaching one is itself a violation. A
// membership with neither INHERIT nor SET reaches nothing and is not.
func TestPostureReportsAnOrdinaryRoleReachableOnlyThroughSetRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.runtimeRole))
	helper, err := containers.RoleName("auth_set_helper", env.instance)
	if err != nil {
		t.Fatal(err)
	}
	helperID := Quote(mustValidatedIdentifier(helper))
	extra := Quote(mustValidatedIdentifier(env.extraColumn(t, ctx, "users")))
	for _, statement := range []string{
		`CREATE ROLE ` + helperID + ` NOLOGIN`,
		`GRANT USAGE ON SCHEMA public TO ` + helperID,
		`GRANT SELECT (` + extra + `) ON public.users TO ` + helperID,
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(env.admin, helper, t.Logf) })

	if _, err := env.admin.Exec(ctx, `GRANT `+helperID+` TO `+role+` WITH INHERIT FALSE, SET FALSE`); err != nil {
		t.Fatal(err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("a membership with neither INHERIT nor SET reaches nothing, yet:\n%s", describe(found))
	}
	if _, err := env.admin.Exec(ctx, `REVOKE `+helperID+` FROM `+role); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(ctx, `GRANT `+helperID+` TO `+role+` WITH INHERIT FALSE, SET TRUE`); err != nil {
		t.Fatal(err)
	}
	conn, err := env.runtime.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET ROLE `+helperID); err != nil {
		t.Fatalf("SET ROLE to the helper: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT `+extra+` FROM public.users`); err != nil {
		t.Fatalf("the helper cannot read the undeclared column the repro relies on: %v", err)
	}
	if found := env.violations(t, ctx); !strings.Contains(describe(found), helper) {
		t.Errorf("a role reachable only through SET ROLE was not reported:\n%s", describe(found))
	}
}
