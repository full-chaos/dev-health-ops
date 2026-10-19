//go:build integration

package authschema

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"
)

// publicReadOracle is the reviewed manifest, written out independently of
// PublicReadColumns (a unit test asserts the two are equal). Every table gets
// one extra column the runtime role must NOT be able to read.
var publicReadOracle = map[string][]string{
	"users": {"id", "email", "password_hash", "full_name", "username", "auth_provider", "auth_provider_id",
		"is_active", "is_verified", "is_superuser", "token_version"},
	"memberships": {"user_id", "org_id", "role"},
	"sso_providers": {"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
		"config", "allowed_domains", "encrypted_secrets"},
	"organizations":          {"id", "slug", "name", "is_active", "tier"},
	"saml_assertion_replays": {"provider_id", "assertion_id", "expires_at"},
	"org_licenses":           {"org_id", "tier", "features_override"},
}

type publicReadsEnv struct {
	*authFixture
	logs *bytes.Buffer
}

func (e *publicReadsEnv) options(logs *bytes.Buffer) Options {
	return Options{Schema: e.schema, RuntimeRole: e.runtimeRole, Logger: slog.New(slog.NewTextHandler(logs, nil))}
}

// newPublicReadsFixture creates stub ops tables in public, owned by the
// migration role (as the real ones are owned by whoever holds GRANT OPTION),
// then re-runs Apply so the grants land.
func newPublicReadsFixture(t *testing.T, ctx context.Context, skip ...string) *publicReadsEnv {
	t.Helper()
	env := newM10Fixture(t, ctx)
	if _, err := env.admin.Exec(ctx, fmt.Sprintf(`GRANT CREATE ON SCHEMA public TO %s`, Quote(mustValidatedIdentifier(env.migrationRole)))); err != nil {
		t.Fatal(err)
	}
	skipped := map[string]bool{}
	for _, name := range skip {
		skipped[name] = true
	}
	tables := make([]string, 0, len(publicReadOracle))
	for table := range publicReadOracle {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		if skipped[table] {
			continue
		}
		columns := make([]string, 0, len(publicReadOracle[table])+1)
		for _, column := range publicReadOracle[table] {
			columns = append(columns, Quote(mustValidatedIdentifier(column))+" text")
		}
		columns = append(columns, "extra_col text")
		if _, err := env.migration.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s (%s)`,
			Quote(mustValidatedIdentifier(table)), strings.Join(columns, ", "))); err != nil {
			t.Fatalf("create stub public.%s: %v", table, err)
		}
	}
	logs := &bytes.Buffer{}
	if _, err := Apply(ctx, env.migration, (&publicReadsEnv{authFixture: env}).options(logs)); err != nil {
		t.Fatalf("Apply over stub public tables: %v", err)
	}
	return &publicReadsEnv{authFixture: env, logs: logs}
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
		for name, statement := range map[string]string{
			"another column": fmt.Sprintf(`SELECT extra_col FROM public.%s`, Quote(mustValidatedIdentifier(table))),
			"every column":   fmt.Sprintf(`SELECT * FROM public.%s`, Quote(mustValidatedIdentifier(table))),
			"an insert":      fmt.Sprintf(`INSERT INTO public.%s (extra_col) VALUES ('x')`, Quote(mustValidatedIdentifier(table))),
			"an update":      fmt.Sprintf(`UPDATE public.%s SET %s = 'x'`, Quote(mustValidatedIdentifier(table)), Quote(mustValidatedIdentifier(columns[0]))),
			"a delete":       fmt.Sprintf(`DELETE FROM public.%s`, Quote(mustValidatedIdentifier(table))),
		} {
			if err := env.runtimeStatement(ctx, statement); err == nil {
				t.Errorf("%s on public.%s was accepted for the runtime role", name, table)
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

func TestPublicReadsExtraColumnPrivilegesAreViolationsAndApplyRemovesThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	role := Quote(mustValidatedIdentifier(env.runtimeRole))

	for name, statement := range map[string]string{
		"SELECT on a column outside the manifest": `GRANT SELECT (extra_col) ON public.users TO ` + role,
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
	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err == nil {
		t.Error("Apply succeeded while the runtime role held a column privilege outside the manifest on an unrelated table")
	}
	if _, err := env.migration.Exec(ctx, `REVOKE ALL ON public.ops_unrelated FROM `+role); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, env.migration, env.options(&bytes.Buffer{})); err != nil {
		t.Fatalf("Apply did not strip the extra privileges on manifest tables: %v", err)
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after Apply is not clean:\n%s", describe(found))
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
	if _, err := env.migration.Exec(ctx, `CREATE TABLE public.org_licenses (org_id text, tier text, features_override text)`); err != nil {
		t.Fatal(err)
	}
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
