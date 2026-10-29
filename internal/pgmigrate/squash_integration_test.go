//go:build integration

package pgmigrate_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// archivedRevisions are the revisions below the squashed baseline that a live database may still record: the old
// baseline 0138 and every archived chain revision but the last (the last is the baseline itself).
func archivedRevisions() []string {
	out := []string{"0138"}
	for revision := 139; revision < 162; revision++ {
		out = append(out, fmt.Sprintf("%04d", revision))
	}
	return out
}

// catalogSnapshot is what a run that executes no DDL leaves unchanged: alembic_version, and every catalog row of a
// relation, function or type outside the system schemas with the transaction that last wrote it.
func catalogSnapshot(t *testing.T, ctx context.Context, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT 'alembic_version ' || version_num FROM alembic_version
UNION ALL
SELECT 'class ' || n.nspname || '.' || c.relname || ' ' || c.xmin::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'
UNION ALL
SELECT 'proc ' || n.nspname || '.' || p.proname || ' ' || p.xmin::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'
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

// A database already at the squashed baseline is at head: the next run applies nothing and executes no DDL; its
// alembic_version rows are the baseline heads before and after.
func TestAtHeadDatabaseRunsNoDDL(t *testing.T) {
	ctx := context.Background()
	instance := sharedInstance(t)
	admin := connect(t, instance.URI)
	conn := connect(t, databaseURI(t, instance.URI, scratchDatabase(t, admin)))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("fresh upgrade: %v", err)
	}
	before := catalogSnapshot(t, ctx, conn)
	result, err := pgmigrate.Upgrade(ctx, conn, baseline, chain)
	if err != nil || result.Action != "up_to_date" || len(result.Applied) != 0 {
		t.Fatalf("second upgrade = %+v, %v; want up_to_date with nothing applied", result, err)
	}
	if after := catalogSnapshot(t, ctx, conn); !reflect.DeepEqual(before, after) {
		t.Fatalf("an up_to_date run changed the catalog or alembic_version")
	}
	var heads []string
	for _, line := range before {
		if strings.HasPrefix(line, "alembic_version ") {
			heads = append(heads, strings.TrimPrefix(line, "alembic_version "))
		}
	}
	if want := pgmigrate.Heads(baseline, chain); !reflect.DeepEqual(heads, want) {
		t.Fatalf("alembic_version holds %v, want the build heads %v", heads, want)
	}
}

// A database that records a revision below the squashed baseline is refused, naming the baseline, before any
// statement: alembic_version and the catalog are as they were.
func TestADatabaseAtAnArchivedRevisionIsRefusedAsBelowTheBaseline(t *testing.T) {
	ctx := context.Background()
	instance := sharedInstance(t)
	admin := connect(t, instance.URI)
	conn := connect(t, databaseURI(t, instance.URI, scratchDatabase(t, admin)))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range archivedRevisions() {
		archivedUpgradeTo(t, ctx, conn, revision)
		before := catalogSnapshot(t, ctx, conn)
		_, err := pgmigrate.Upgrade(ctx, conn, baseline, chain)
		if err == nil || !strings.Contains(err.Error(), "below the squashed baseline 0162") {
			t.Fatalf("upgrade of a database at %s = %v, want the below-baseline refusal", revision, err)
		}
		if after := catalogSnapshot(t, ctx, conn); !reflect.DeepEqual(before, after) {
			t.Fatalf("the refusal at %s changed the database", revision)
		}
	}
}
