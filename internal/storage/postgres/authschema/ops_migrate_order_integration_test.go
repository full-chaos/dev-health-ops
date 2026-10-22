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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A fresh install can start the auth migrator and dho migrate in either order.
// The auth lineage owns only its own schema; dho owns public. These tests run
// both orders on an empty database, and the refusals that must stay refusals.

type orderFixture struct {
	ctx  context.Context
	uri  string
	role string
}

func orderConnect(t *testing.T, ctx context.Context, uri string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func newOrderFixture(t *testing.T) *orderFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	admin := orderConnect(t, ctx, instance.URI)
	// The role is cluster-global: derive it from this instance's scratch database
	// and drop it afterwards, so a shared cluster (DEV_HEALTH_TEST_POSTGRES_DSN)
	// never sees two tests claim one name.
	role, err := containers.RoleName("auth_order_rt", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN`, Quote(mustValidatedIdentifier(role)))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	return &orderFixture{ctx: ctx, uri: instance.URI, role: role}
}

func (f *orderFixture) authMigrate(t *testing.T, schema string) {
	t.Helper()
	pool, err := pgxpool.New(f.ctx, f.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := Apply(f.ctx, pool, Options{Schema: schema, RuntimeRole: f.role}); err != nil {
		t.Fatalf("auth migrate (schema %s): %v", schema, err)
	}
}

func (f *orderFixture) dhoMigrate(t *testing.T) (pgmigrate.Result, error) {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	conn := orderConnect(t, f.ctx, f.uri)
	return pgmigrate.Upgrade(f.ctx, conn, baseline, chain)
}

// dhoMigrateLogged is dhoMigrate with its log lines captured.
func (f *orderFixture) dhoMigrateLogged(t *testing.T) (pgmigrate.Result, string, error) {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	result, err := pgmigrate.UpgradeLogged(f.ctx, orderConnect(t, f.ctx, f.uri), baseline, chain, slog.New(slog.NewTextHandler(&logs, nil)))
	return result, logs.String(), err
}

// lineageRows is every row of the auth lineage table, values included.
func (f *orderFixture) lineageRows(t *testing.T, schema string) string {
	t.Helper()
	conn := orderConnect(t, f.ctx, f.uri)
	var rows string
	if err := conn.QueryRow(f.ctx, `SELECT coalesce(string_agg(version::text || '|' || name || '|' || applied_at::text, ';' ORDER BY version), '') FROM `+schema+`.schema_migrations`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func (f *orderFixture) count(t *testing.T, query string) int {
	t.Helper()
	conn := orderConnect(t, f.ctx, f.uri)
	var n int
	if err := conn.QueryRow(f.ctx, query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestOpsMigrateSucceedsAfterAuthMigrateOnAnEmptyDatabase(t *testing.T) {
	for _, schema := range []string{"auth", "identity_plane"} {
		t.Run(schema, func(t *testing.T) {
			// dho migrate learns the auth schema's name from the same variable
			// auth-migrate reads.
			t.Setenv("AUTH_DATABASE_SCHEMA", schema)
			f := newOrderFixture(t)
			f.authMigrate(t, schema)
			// The marker matches what the auth migrator really creates: the
			// database still reads as empty to the ops migrator.
			baseline, err := pgmigrate.LoadBaseline()
			if err != nil {
				t.Fatal(err)
			}
			chain, err := pgmigrate.LoadChain()
			if err != nil {
				t.Fatal(err)
			}
			if status, err := pgmigrate.ReadStatus(f.ctx, orderConnect(t, f.ctx, f.uri), baseline, chain); err != nil || status.State != "empty" {
				t.Fatalf("status after auth-migrate = %q, %v; want empty", status.State, err)
			}
			rowsBefore := f.lineageRows(t, schema)
			result, logs, err := f.dhoMigrateLogged(t)
			if err != nil {
				t.Fatalf("dho migrate after auth-migrate: %v", err)
			}
			if rowsAfter := f.lineageRows(t, schema); rowsBefore == "" || rowsAfter != rowsBefore {
				t.Errorf("the auth lineage rows changed:\nbefore %s\nafter  %s", rowsBefore, rowsAfter)
			}
			// One Info line says which schema was not counted, and how many objects
			// it held, so a wrong exemption is visible without a debugger.
			if !strings.Contains(logs, "schema="+schema) || !strings.Contains(logs, "objects=") {
				t.Errorf("no Info line names the exempted schema and its object count:\n%s", logs)
			}
			if result.Action != "baseline_applied" {
				t.Errorf("action = %q, want baseline_applied", result.Action)
			}
			if n := f.count(t, `SELECT count(*) FROM `+schema+`.schema_migrations`); n == 0 {
				t.Error("the auth lineage rows are gone")
			}
			if n := f.count(t, `SELECT count(*) FROM public.alembic_version`); n == 0 {
				t.Error("dho recorded no alembic_version")
			}
			if again, err := f.dhoMigrate(t); err != nil || again.Action != "up_to_date" {
				t.Errorf("second dho migrate = %q, %v; want up_to_date", again.Action, err)
			}
			// Re-running the auth migrator now that the ops tables exist grants the
			// runtime role its column reads on them and still succeeds.
			f.authMigrate(t, schema)
		})
	}
}

func TestAuthMigrateSucceedsAfterOpsMigrateOnAnEmptyDatabase(t *testing.T) {
	f := newOrderFixture(t)
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatalf("dho migrate: %v", err)
	}
	f.authMigrate(t, "auth")
	if again, err := f.dhoMigrate(t); err != nil || again.Action != "up_to_date" {
		t.Errorf("dho migrate after auth-migrate = %q, %v; want up_to_date", again.Action, err)
	}
}

// Only the auth lineage's own schema is ignored. Anything else stays foreign.
func TestOpsMigrateStillRefusesForeignObjectsNextToAnAuthSchema(t *testing.T) {
	mismatch := func(t *testing.T, err error, found, configured string) {
		t.Helper()
		var refusal pgmigrate.AuthSchemaMismatchError
		if !errors.As(err, &refusal) {
			t.Fatalf("err = %v, want an AuthSchemaMismatchError", err)
		}
		if refusal.Found != found || refusal.Configured != configured {
			t.Errorf("mismatch = found %q configured %q, want found %q configured %q", refusal.Found, refusal.Configured, found, configured)
		}
		if message := refusal.Error(); !strings.Contains(message, found) || !strings.Contains(message, "AUTH_DATABASE_SCHEMA") {
			t.Errorf("the refusal does not name the schema it found and the variable to set: %s", message)
		}
	}
	foreign := func(t *testing.T, err error) {
		t.Helper()
		var refusal pgmigrate.ForeignDatabaseError
		if !errors.As(err, &refusal) {
			t.Fatalf("err = %v, want a ForeignDatabaseError", err)
		}
	}
	t.Run("a Supabase-style auth schema is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		conn := orderConnect(t, f.ctx, f.uri)
		for _, statement := range []string{
			`CREATE SCHEMA auth`,
			`CREATE TABLE auth.schema_migrations (version varchar(255) PRIMARY KEY)`,
			`CREATE TABLE auth.users (id uuid PRIMARY KEY)`,
		} {
			if _, err := conn.Exec(f.ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("a look-alike with the wrong column type is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		conn := orderConnect(t, f.ctx, f.uri)
		for _, statement := range []string{
			`CREATE SCHEMA auth`,
			`CREATE TABLE auth.schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamp NOT NULL)`,
		} {
			if _, err := conn.Exec(f.ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("an object in public next to a real auth schema is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		f.authMigrate(t, "auth")
		conn := orderConnect(t, f.ctx, f.uri)
		if _, err := conn.Exec(f.ctx, `CREATE TABLE public.legacy_thing (id int)`); err != nil {
			t.Fatal(err)
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("an exact-shape bookkeeping table in public does not exempt public", func(t *testing.T) {
		f := newOrderFixture(t)
		conn := orderConnect(t, f.ctx, f.uri)
		for _, statement := range []string{
			`CREATE TABLE public.schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL)`,
			`CREATE TABLE public.foreign_thing (id integer)`,
		} {
			if _, err := conn.Exec(f.ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("an exact-shape bookkeeping table in a schema that is not the configured one is refused as an auth schema mismatch", func(t *testing.T) {
		t.Setenv("AUTH_DATABASE_SCHEMA", "auth")
		f := newOrderFixture(t)
		conn := orderConnect(t, f.ctx, f.uri)
		for _, statement := range []string{
			`CREATE SCHEMA lookalike`,
			`CREATE TABLE lookalike.schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL)`,
			`CREATE TABLE lookalike.things (id integer)`,
		} {
			if _, err := conn.Exec(f.ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.dhoMigrate(t)
		mismatch(t, err, "lookalike", "auth")
	})
	t.Run("the configured auth schema is exempt only under its configured name", func(t *testing.T) {
		t.Setenv("AUTH_DATABASE_SCHEMA", "identity_plane")
		f := newOrderFixture(t)
		f.authMigrate(t, "auth")
		_, err := f.dhoMigrate(t)
		mismatch(t, err, "auth", "identity_plane")
	})
	t.Run("a reserved-word schema name never exempts anything, because the auth migrator could not own it", func(t *testing.T) {
		t.Setenv("AUTH_DATABASE_SCHEMA", "with")
		f := newOrderFixture(t)
		conn := orderConnect(t, f.ctx, f.uri)
		for _, statement := range []string{
			`CREATE SCHEMA "with"`,
			`CREATE TABLE "with".schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL)`,
			`CREATE TABLE "with".foreign_object (id integer)`,
		} {
			if _, err := conn.Exec(f.ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("an unrelated schema next to a real auth schema is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		f.authMigrate(t, "auth")
		conn := orderConnect(t, f.ctx, f.uri)
		if _, err := conn.Exec(f.ctx, `CREATE SCHEMA other; CREATE TABLE other.t (id int)`); err != nil {
			t.Fatal(err)
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
}

// When the two migrators disagree about the auth schema's name the ops migrator
// refuses with a cause of its own, in every surface it reports through.
func TestOpsMigrateRefusesWithTheCauseWhenItIsNotToldTheAuthSchema(t *testing.T) {
	t.Setenv("AUTH_DATABASE_SCHEMA", "")
	f := newOrderFixture(t)
	f.authMigrate(t, "identity_plane")

	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	status, err := pgmigrate.ReadStatus(f.ctx, orderConnect(t, f.ctx, f.uri), baseline, chain)
	if err != nil || status.State != "auth_schema_mismatch" {
		t.Errorf("status = %q, %v; want auth_schema_mismatch", status.State, err)
	}
	_, upgradeErr := f.dhoMigrate(t)
	var refusal pgmigrate.AuthSchemaMismatchError
	if !errors.As(upgradeErr, &refusal) || refusal.Found != "identity_plane" {
		t.Fatalf("upgrade err = %v, want an AuthSchemaMismatchError naming identity_plane", upgradeErr)
	}
	var foreign pgmigrate.ForeignDatabaseError
	if errors.As(upgradeErr, &foreign) {
		t.Error("the refusal is also a generic foreign-database error")
	}
	// Told the name, the same database migrates.
	t.Setenv("AUTH_DATABASE_SCHEMA", "identity_plane")
	if result, err := f.dhoMigrate(t); err != nil || result.Action != "baseline_applied" {
		t.Errorf("dho migrate with the variable set = %q, %v; want baseline_applied", result.Action, err)
	}
}

func (f *orderFixture) grantsPass(t *testing.T, schema string) error {
	t.Helper()
	pool, err := pgxpool.New(f.ctx, f.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = Grants(f.ctx, pool, Options{Schema: schema, RuntimeRole: f.role})
	return err
}

// unreadablePublicColumns lists "table.column" for every PublicReadColumns
// column the runtime role cannot SELECT (a missing table counts as unreadable).
func (f *orderFixture) unreadablePublicColumns(t *testing.T) []string {
	t.Helper()
	conn := orderConnect(t, f.ctx, f.uri)
	var missing []string
	for _, entry := range PublicReadColumns() {
		var exists bool
		if err := conn.QueryRow(f.ctx, `SELECT to_regclass(format('public.%I', $1::text)) IS NOT NULL`, entry.Table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		for _, column := range entry.Columns {
			readable := false
			if exists {
				if err := conn.QueryRow(f.ctx, `SELECT has_column_privilege($1, format('public.%I', $2::text), $3, 'SELECT')`,
					f.role, entry.Table, column).Scan(&readable); err != nil {
					t.Fatal(err)
				}
			}
			if !readable {
				missing = append(missing, entry.Table+"."+column)
			}
		}
	}
	return missing
}

// CHAOS-10581: the chart runs auth-migrate before ops migrate, so the public
// read grants are skipped. The grants pass after ops migrate closes the gap.
func TestGrantsPassAfterAuthThenOpsMigrateGrantsPublicReads(t *testing.T) {
	f := newOrderFixture(t)
	f.authMigrate(t, "auth")
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatalf("dho migrate: %v", err)
	}
	if missing := f.unreadablePublicColumns(t); len(missing) == 0 {
		t.Fatal("auth-migrate then ops migrate already granted every public read: the gap this test guards is gone")
	}
	if err := f.grantsPass(t, "auth"); err != nil {
		t.Fatalf("grants pass: %v", err)
	}
	if missing := f.unreadablePublicColumns(t); len(missing) != 0 {
		t.Fatalf("columns still unreadable after the grants pass: %v", missing)
	}
	if err := f.grantsPass(t, "auth"); err != nil {
		t.Fatalf("second grants pass: %v", err)
	}
}

// Upgrade: a listed table that the release's ops migration created has no
// grant; the grants pass adds it without touching the lineage.
func TestGrantsPassRestoresAMissingGrantOnUpgrade(t *testing.T) {
	f := newOrderFixture(t)
	f.authMigrate(t, "auth")
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatalf("dho migrate: %v", err)
	}
	if err := f.grantsPass(t, "auth"); err != nil {
		t.Fatal(err)
	}
	conn := orderConnect(t, f.ctx, f.uri)
	if _, err := conn.Exec(f.ctx, fmt.Sprintf(`REVOKE ALL ON TABLE public.org_licenses FROM %s`, Quote(mustValidatedIdentifier(f.role)))); err != nil {
		t.Fatal(err)
	}
	if missing := f.unreadablePublicColumns(t); len(missing) == 0 {
		t.Fatal("the revoke did not remove the grant")
	}
	rowsBefore := f.lineageRows(t, "auth")
	if err := f.grantsPass(t, "auth"); err != nil {
		t.Fatal(err)
	}
	if missing := f.unreadablePublicColumns(t); len(missing) != 0 {
		t.Fatalf("still unreadable: %v", missing)
	}
	if rowsAfter := f.lineageRows(t, "auth"); rowsAfter != rowsBefore {
		t.Errorf("the grants pass changed the lineage rows:\n%s\n%s", rowsBefore, rowsAfter)
	}
}

// The grants pass is not the default run's skip-with-warn: a missing listed
// table fails it, naming the table.
func TestGrantsPassFailsLoudWhenAListedTableIsMissing(t *testing.T) {
	f := newOrderFixture(t)
	f.authMigrate(t, "auth")
	err := f.grantsPass(t, "auth")
	if !errors.Is(err, ErrPublicTableMissing) || !strings.Contains(err.Error(), "public.") {
		t.Fatalf("grants pass on a database without ops tables = %v, want ErrPublicTableMissing naming the table", err)
	}
}

func TestGrantsPassRefusesAnUnmigratedAuthSchema(t *testing.T) {
	f := newOrderFixture(t)
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatal(err)
	}
	if err := f.grantsPass(t, "auth"); err == nil || !errors.Is(err, ErrMigrationFailed) {
		t.Fatalf("grants pass before auth-migrate = %v, want ErrMigrationFailed", err)
	}
}
