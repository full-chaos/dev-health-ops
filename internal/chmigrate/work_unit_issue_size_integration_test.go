//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9619: migration 130 creates work_unit_issue_size and rollback/130_*.down.sql drops it. The test
// rehearses both: the upgrade runs twice (re-runnable), the newest row per unit wins after a merge, the
// downgrade removes the table, and the upgrade runs again.
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
	run := func(path string) {
		t.Helper()
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range chmigrate.SplitStatements(string(body)) {
			exec(statement)
		}
	}
	tables := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'work_unit_issue_size'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	const up = "sql/130_work_unit_issue_size.sql"
	const down = "rollback/130_work_unit_issue_size.down.sql"
	run(up)
	run(up)
	if got := tables(); got != 1 {
		t.Fatalf("after the upgrade work_unit_issue_size exists %d times, want 1", got)
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

	run(down)
	if got := tables(); got != 0 {
		t.Fatalf("after the downgrade work_unit_issue_size exists %d times, want 0", got)
	}
	run(up)
	if got := tables(); got != 1 {
		t.Fatalf("after a second upgrade work_unit_issue_size exists %d times, want 1", got)
	}
}
