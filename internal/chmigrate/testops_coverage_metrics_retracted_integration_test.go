//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const coverageVersion = "162_testops_coverage_metrics_retracted.sql"

// The coverage baseline read of the build before 162 (internal/queryapi/coveragebaselines newestPerRepoDay) with its bindings
// written in: the newest stored row of each repository day.
const coverageImageRead = `SELECT count(), countIf(line_pct IS NOT NULL) FROM (
    SELECT repo_id, day, (argMax(tuple(line_coverage_pct), computed_at)).1 AS line_pct
    FROM testops_coverage_metrics_daily
    WHERE org_id = 'org-160' AND day >= '2026-08-10' AND day < '2026-08-12'
    GROUP BY repo_id, day)`

// The writer of the build before 162 names its columns and never names retracted.
const coverageImageInsert = `INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, branch_coverage_pct, uncovered_files_count,
  coverage_regression_count, team_id, org_id, computed_at)
VALUES (toUUID('14008000-0000-4000-8000-0000000001b1'), '2026-08-11', 80, 70, 0, 0, 'team-1', 'org-160', '2026-08-13 10:00:00')`

func coverageRetractedColumn(ctx context.Context, t *testing.T, conn driver.Conn) string {
	t.Helper()
	var column string
	rows, err := conn.Query(ctx, `SELECT type || ' ' || default_kind || ' ' || default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'testops_coverage_metrics_daily' AND name = 'retracted'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
	}
	return column
}

// INVARIANT (CHAOS-14008): the rollback of 162 is "images back on a build without 162, then 162's down file". Up, down, up on a
// real ClickHouse: the column is gone after the down file and its version row too; the read and the INSERT of the build before 162
// work on the table in that shape (a marker row reads as a stored row with NULL percentages, as the down file says); the upgrade
// applies 162 again.
func TestRollback162UpDownUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if got := coverageRetractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("at the head the retracted column is %q, want UInt8 DEFAULT 0", got)
	}
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO testops_coverage_metrics_daily (repo_id, day, line_coverage_pct, branch_coverage_pct, uncovered_files_count,
  coverage_regression_count, team_id, org_id, computed_at) VALUES (toUUID('14008000-0000-4000-8000-0000000001b1'), '2026-08-10', 60, 50, 0, 0, 'team-1', 'org-160', '2026-08-12 10:00:00')`)
	exec(`INSERT INTO testops_coverage_metrics_daily (repo_id, day, uncovered_files_count, coverage_regression_count, team_id, org_id, computed_at, retracted)
VALUES (toUUID('14008000-0000-4000-8000-0000000001b1'), '2026-08-10', 0, 0, 'team-1', 'org-160', '2026-08-12 11:00:00', 1)`)
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", coverageVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 162")
	}

	body, err := os.ReadFile("rollback/162_testops_coverage_metrics_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if got := coverageRetractedColumn(ctx, t, conn); got != "" {
		t.Fatalf("after the down file the retracted column is still %q", got)
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 162")
	}

	var days, withLine uint64
	if err := conn.QueryRow(ctx, coverageImageRead).Scan(&days, &withLine); err != nil {
		t.Fatalf("the read of the build before 162 fails on the rolled-back table: %v", err)
	}
	if days != 1 || withLine != 0 {
		t.Fatalf("the older read gives %d day(s) with %d line value(s), want the marker row as a stored row with no value (1, 0)", days, withLine)
	}
	exec(coverageImageInsert)
	if err := conn.QueryRow(ctx, coverageImageRead).Scan(&days, &withLine); err != nil {
		t.Fatal(err)
	}
	if days != 2 || withLine != 1 {
		t.Fatalf("after the older writer's row the older read gives %d day(s) with %d line value(s), want 2 and 1", days, withLine)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != coverageVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 162 applied", result, err)
	}
	if got := coverageRetractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("after the upgrade again the retracted column is %q", got)
	}
	var live uint64
	if err := conn.QueryRow(ctx, "SELECT countIf(retracted = 0) FROM testops_coverage_metrics_daily WHERE org_id = 'org-160'").Scan(&live); err != nil || live != 3 {
		t.Fatalf("after the upgrade again %d rows read retracted = 0 (err %v), want all 3 stored rows (the marker row is a stored row with no value now)", live, err)
	}
}
