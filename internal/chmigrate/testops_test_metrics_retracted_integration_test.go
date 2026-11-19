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

const retractedOrg = "org-165"

// retractedImageRead is the cases read of the build before 165 (internal/queryapi/testopswindow/testopswindow.go:132-151 at
// origin/main e16b0441, casesStatement) with its bindings written in: the newest stored row of each repository day, summed.
const retractedImageRead = `SELECT sum(passed_count), sum(failed_count), sum(total_cases), countIf(flake_cases IS NULL OR distinct_cases IS NULL)
FROM (
    SELECT * FROM testops_test_metrics_daily
    WHERE org_id = 'org-165' AND day >= '2026-08-10' AND day <= '2026-08-11'
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, day
)`

// The writer of the build before 165 names its columns and never names retracted.
const retractedImageInsert = `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, pass_rate,
  failure_rate, total_suites, flake_cases, distinct_cases, team_id, computed_at)
VALUES ('org-165', toUUID('13928000-0000-4000-8000-0000000001b1'), '2026-08-11', 2, 2, 0, 1, 0, 1, 0, 2, 'team-1', '2026-08-13 10:00:00')`

func retractedColumn(ctx context.Context, t *testing.T, conn driver.Conn) string {
	t.Helper()
	var column string
	rows, err := conn.Query(ctx, `SELECT type || ' ' || default_kind || ' ' || default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'testops_test_metrics_daily' AND name = 'retracted'`)
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

// INVARIANT (CHAOS-13928): the rollback of 165 is "images back on a build without 165, then 165's down file". Up, down, up on a
// real ClickHouse: the column is gone after the down file and its version row too; the read and the INSERT of the build before 165
// work on the table in that shape (a marker row reads as a stored row of zeros, as the down file says); the upgrade applies 165 again.
func TestRollback165UpDownUp(t *testing.T) {
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
	if got := retractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("at the head the retracted column is %q, want UInt8 DEFAULT 0", got)
	}
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
  total_suites, flake_cases, distinct_cases, team_id, computed_at) VALUES ('org-165', toUUID('13928000-0000-4000-8000-0000000001b1'), '2026-08-10', 4, 2, 2, 0.5, 0.5, 1, 1, 2, 'team-1', '2026-08-12 10:00:00')`)
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, team_id, computed_at, retracted)
VALUES ('org-165', toUUID('13928000-0000-4000-8000-0000000001b1'), '2026-08-10', 'team-1', '2026-08-12 11:00:00', 1)`)
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", "165_testops_test_metrics_retracted.sql").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 165")
	}

	body, err := os.ReadFile("rollback/165_testops_test_metrics_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if got := retractedColumn(ctx, t, conn); got != "" {
		t.Fatalf("after the down file the retracted column is still %q", got)
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 165")
	}

	var passed, failed, total, withoutCounts uint64
	if err := conn.QueryRow(ctx, retractedImageRead).Scan(&passed, &failed, &total, &withoutCounts); err != nil {
		t.Fatalf("the read of the build before 165 fails on the rolled-back table: %v", err)
	}
	if total != 0 || passed != 0 || failed != 0 {
		t.Fatalf("the older read gives passed %d failed %d total %d, want the marker row as a stored row of zeros (0, 0, 0)", passed, failed, total)
	}
	exec(retractedImageInsert)
	if err := conn.QueryRow(ctx, retractedImageRead).Scan(&passed, &failed, &total, &withoutCounts); err != nil {
		t.Fatal(err)
	}
	if total != 2 || passed != 2 {
		t.Fatalf("after the older writer's row the older read gives passed %d total %d, want 2 and 2", passed, total)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != "165_testops_test_metrics_retracted.sql" {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 165 applied", result, err)
	}
	if got := retractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("after the upgrade again the retracted column is %q", got)
	}
	var live uint64
	if err := conn.QueryRow(ctx, "SELECT countIf(retracted = 0) FROM testops_test_metrics_daily WHERE org_id = 'org-165'").Scan(&live); err != nil || live != 3 {
		t.Fatalf("after the upgrade again %d rows read retracted = 0 (err %v), want all 3 stored rows (the marker row is a stored zero row now)", live, err)
	}
}
