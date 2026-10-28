//go:build integration

package pgmigrate_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/storage/roleprovision"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ownerRows names the owner of every function, relation, schema and type outside the system schemas: pg_dump
// --no-owner leaves owners out of the schema compare, and a SECURITY DEFINER function runs as its owner.
func ownerRows(t *testing.T, ctx context.Context, conn *pgx.Conn) []string {
	t.Helper()
	const system = `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'`
	rows, err := conn.Query(ctx, `
SELECT 'function ' || n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ') ' || pg_get_userbyid(p.proowner)
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE `+system+`
UNION ALL
SELECT 'relation ' || c.relkind::text || ' ' || n.nspname || '.' || c.relname || ' ' || pg_get_userbyid(c.relowner)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE `+system+`
UNION ALL
SELECT 'schema ' || n.nspname || ' ' || pg_get_userbyid(n.nspowner) FROM pg_namespace n WHERE `+system+`
UNION ALL
SELECT 'type ' || n.nspname || '.' || y.typname || ' ' || pg_get_userbyid(y.typowner)
  FROM pg_type y JOIN pg_namespace n ON n.oid = y.typnamespace WHERE `+system+` AND y.typrelid = 0 AND y.typcategory <> 'A'
ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// riverAndRoles adds what other migrators put in the same database: the River schema (River's own migrator) and the
// runtime roles' grants (dho migrate roles), so the compare covers both over each schema.
func riverAndRoles(t *testing.T, ctx context.Context, instance *containers.Instance, uri string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Schema: "river",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	name := func(base string) string {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		return role
	}
	options := roleprovision.Options{
		Domain:      roleprovision.Role{Name: name("sq_domain"), Password: "squash-test-only"},
		Queue:       roleprovision.Role{Name: name("sq_queue"), Password: "squash-test-only"},
		Coordinator: roleprovision.Role{Name: name("sq_coord"), Password: "squash-test-only"},
		QueryAPI:    roleprovision.Role{Name: name("sq_qapi"), Password: "squash-test-only"},
		RiverSchema: "river",
	}
	if err := roleprovision.Apply(ctx, pool, options); err != nil {
		t.Fatalf("dho migrate roles: %v", err)
	}
}

// TestBaselineEqualsTheArchivedChain is the squash's equivalence (D6430): a database built from the old 0138
// baseline and the archived chain 0139..0162 (A), and one built from baseline/head.json (B), are the same database --
// schema with privileges, seed rows, alembic heads, the owner of every object, and, after River's migrator and the
// role grants ran on both, the whole database. Any chain revision above the baseline runs on both.
func TestBaselineEqualsTheArchivedChain(t *testing.T) {
	ctx := context.Background()
	instance := isolatedInstance(t)
	admin := connect(t, instance.URI)
	oldBaseline, archived := archivedBuild(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}

	aDB := scratchDatabase(t, admin)
	aURI := databaseURI(t, instance.URI, aDB)
	aConn := connect(t, aURI)
	aStarted := time.Now()
	if _, err := pgmigrate.Upgrade(ctx, aConn, oldBaseline, append(append([]pgmigrate.ChainFile(nil), archived...), chain...)); err != nil {
		t.Fatalf("build A (0138 + archived chain): %v", err)
	}
	aCapture := capture(t, ctx, instance, aDB, window{aStarted, time.Now()})
	aCapture.Cutover, aCapture.RiverSchema = productionSettings.Cutover, productionSettings.RiverSchema
	if os.Getenv(updateEnv) == "1" {
		if len(chain) > 0 {
			t.Fatal("the baseline is regenerated only with no chain revision above it")
		}
		writeBaseline(t, aCapture)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(updateEnv) == "1" {
		baseline = aCapture // the embedded copy is the one this binary was built with
	}

	bDB := scratchDatabase(t, admin)
	bURI := databaseURI(t, instance.URI, bDB)
	bConn := connect(t, bURI)
	bStarted := time.Now()
	result, err := pgmigrate.Upgrade(ctx, bConn, baseline, chain)
	if err != nil || result.Action != "baseline_applied" || len(result.Applied) != len(chain) {
		t.Fatalf("build B (baseline/head.json) = %+v, %v", result, err)
	}
	bCapture := capture(t, ctx, instance, bDB, window{bStarted, time.Now()})
	bCapture.Cutover, bCapture.RiverSchema = baseline.Cutover, baseline.RiverSchema

	if !reflect.DeepEqual(aCapture.Heads, bCapture.Heads) || !reflect.DeepEqual(bCapture.Heads, pgmigrate.Heads(baseline, chain)) {
		t.Fatalf("heads: A %v, B %v, build %v", aCapture.Heads, bCapture.Heads, pgmigrate.Heads(baseline, chain))
	}
	if diff := compare(roundTrip(t, ctx, instance, admin, aCapture), roundTrip(t, ctx, instance, admin, bCapture)); diff != "" {
		t.Fatalf("baseline/head.json builds a different database than 0138 + the archived chain: %s", diff)
	}
	if a, b := ownerRows(t, ctx, aConn), ownerRows(t, ctx, bConn); !reflect.DeepEqual(a, b) {
		t.Fatalf("object owners differ:\nA %v\nB %v", a, b)
	}

	riverAndRoles(t, ctx, instance, aURI)
	aFull := capture(t, ctx, instance, aDB, window{aStarted, time.Now()})
	riverAndRoles(t, ctx, instance, bURI)
	bFull := capture(t, ctx, instance, bDB, window{bStarted, time.Now()})
	if diff := compare(roundTrip(t, ctx, instance, admin, aFull), roundTrip(t, ctx, instance, admin, bFull)); diff != "" {
		t.Fatalf("after River and the role grants, the two databases differ: %s", diff)
	}
	if a, b := ownerRows(t, ctx, aConn), ownerRows(t, ctx, bConn); !reflect.DeepEqual(a, b) {
		t.Fatalf("object owners differ after River and the role grants:\nA %v\nB %v", a, b)
	}
}
