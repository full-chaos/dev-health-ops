//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9807: migration 130 creates work_unit_issue_size and rollback/130_*.down.sql drops it. The test rehearses
// both through the production migrator (chmigrate.Upgrade, what `dho migrate clickhouse upgrade` runs): the
// upgrade records the version and creates the table, a second upgrade is up to date, the newest row per unit wins
// after a merge, the operator rollback removes the table AND its version row, and the next upgrade creates the
// table again.
func TestWorkUnitIssueSizeMigrationUpgradesAndDowngrades(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	count := func(query string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	tables := func() uint64 {
		return count("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'work_unit_issue_size'")
	}
	recorded := func() uint64 {
		return count("SELECT count() FROM schema_migrations WHERE version = '130_work_unit_issue_size.sql'")
	}

	chschema.Apply(ctx, t, instance)
	if tables() != 1 || recorded() == 0 {
		t.Fatalf("after the upgrade: table %d, version rows %d; want the table and its version", tables(), recorded())
	}
	chschema.Apply(ctx, t, instance)
	if tables() != 1 {
		t.Fatalf("a second upgrade left %d tables", tables())
	}
	exec(`INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at)
VALUES ('o', 'u', 3, 1, 2, 'r1', '2026-10-01 00:00:00'), ('o', 'u', 8, 2, 2, 'r2', '2026-10-02 00:00:00')`)
	exec(`OPTIMIZE TABLE work_unit_issue_size FINAL`)
	var size float64
	var rows uint64
	if err := conn.QueryRow(ctx, "SELECT any(issue_size), count() FROM work_unit_issue_size WHERE org_id = 'o' AND work_unit_id = 'u'").Scan(&size, &rows); err != nil {
		t.Fatal(err)
	}
	if size != 8 || rows != 1 {
		t.Fatalf("after a merge the unit holds %d rows of size %v, want the newest row only (size 8)", rows, size)
	}

	body, err := os.ReadFile("rollback/130_work_unit_issue_size.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if tables() != 0 || recorded() != 0 {
		t.Fatalf("after the rollback: table %d, version rows %d; want neither", tables(), recorded())
	}

	chschema.Apply(ctx, t, instance)
	if tables() != 1 || recorded() == 0 {
		t.Fatalf("the upgrade after the rollback: table %d, version rows %d; want the table back", tables(), recorded())
	}
}
