//go:build integration

package pgmigrate_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A fresh install can start the auth migrator and dho migrate in either order.
// The auth lineage owns only its own schema; dho owns public. These tests run
// both orders on an empty database, and the refusals that must stay refusals.

type orderFixture struct {
	ctx      context.Context
	admin    *pgx.Conn
	uri      string
	database string
	role     string
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
	admin := connect(t, instance.URI)
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	role := "auth_order_rt_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN"); err != nil {
		t.Fatal(err)
	}
	database := scratchDatabase(t, admin)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })
	return &orderFixture{ctx: ctx, admin: admin, uri: databaseURI(t, instance.URI, database), database: database, role: role}
}

func (f *orderFixture) authMigrate(t *testing.T, schema string) {
	t.Helper()
	pool, err := pgxpool.New(f.ctx, f.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := authschema.Apply(f.ctx, pool, authschema.Options{Schema: schema, RuntimeRole: f.role}); err != nil {
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
	conn := connect(t, f.uri)
	return pgmigrate.Upgrade(f.ctx, conn, baseline, chain)
}

func (f *orderFixture) count(t *testing.T, query string) int {
	t.Helper()
	conn := connect(t, f.uri)
	var n int
	if err := conn.QueryRow(f.ctx, query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestOpsMigrateSucceedsAfterAuthMigrateOnAnEmptyDatabase(t *testing.T) {
	for _, schema := range []string{"auth", "identity_plane"} {
		t.Run(schema, func(t *testing.T) {
			f := newOrderFixture(t)
			f.authMigrate(t, schema)
			result, err := f.dhoMigrate(t)
			if err != nil {
				t.Fatalf("dho migrate after auth-migrate: %v", err)
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
	foreign := func(t *testing.T, err error) {
		t.Helper()
		var refusal pgmigrate.ForeignDatabaseError
		if !errors.As(err, &refusal) {
			t.Fatalf("err = %v, want a ForeignDatabaseError", err)
		}
	}
	t.Run("a Supabase-style auth schema is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		conn := connect(t, f.uri)
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
		conn := connect(t, f.uri)
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
		conn := connect(t, f.uri)
		if _, err := conn.Exec(f.ctx, `CREATE TABLE public.legacy_thing (id int)`); err != nil {
			t.Fatal(err)
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
	t.Run("an unrelated schema next to a real auth schema is foreign", func(t *testing.T) {
		f := newOrderFixture(t)
		f.authMigrate(t, "auth")
		conn := connect(t, f.uri)
		if _, err := conn.Exec(f.ctx, `CREATE SCHEMA other; CREATE TABLE other.t (id int)`); err != nil {
			t.Fatal(err)
		}
		_, err := f.dhoMigrate(t)
		foreign(t, err)
	})
}
