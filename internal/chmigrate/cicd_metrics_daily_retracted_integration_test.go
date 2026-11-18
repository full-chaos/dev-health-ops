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

// cicdRetractedImageRead is the stored-row read of the build before 159 (internal/jobs/report/dedup.go dedupFromSource for
// cicd_metrics_daily, summed as the pipelines_count chart sums it): the newest stored row of each repository day.
const cicdRetractedImageRead = `SELECT toFloat64(sum(pipelines_count)), count()
FROM (SELECT * FROM cicd_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS cicd_metrics_daily
WHERE org_id = 'org-159' AND day >= '2026-08-10' AND day <= '2026-08-11'`

// The writer of the build before 159 names its columns and never names retracted (internal/jobs/metrics/daily/cicd/clickhouse.go).
const cicdRetractedImageInsert = `INSERT INTO cicd_metrics_daily (repo_id, day, pipelines_count, success_rate, avg_duration_minutes,
  p90_duration_minutes, avg_queue_minutes, computed_at, org_id)
VALUES (toUUID('14016000-0000-4000-8000-0000000001b1'), '2026-08-11', 2, 1, 10, 10, 1, '2026-08-13 10:00:00', 'org-159')`

func cicdRetractedColumn(ctx context.Context, t *testing.T, conn driver.Conn) string {
	t.Helper()
	var column string
	rows, err := conn.Query(ctx, `SELECT type || ' ' || default_kind || ' ' || default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'cicd_metrics_daily' AND name = 'retracted'`)
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

// INVARIANT (CHAOS-14016): the rollback of 159 is "image back on a build without 159, then 159's down file". Up, down, up on a
// real ClickHouse: the column is gone after the down file and its version row too; the read and the INSERT of the build before
// 159 work on the table in that shape (a marker row reads as a stored row of zeros, as the down file says); the upgrade applies
// 159 again.
func TestRollback159UpDownUp(t *testing.T) {
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
	if got := cicdRetractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("at the head the retracted column is %q, want UInt8 DEFAULT 0", got)
	}
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO cicd_metrics_daily (repo_id, day, pipelines_count, success_rate, computed_at, org_id)
VALUES (toUUID('14016000-0000-4000-8000-0000000001b1'), '2026-08-10', 3, 0.5, '2026-08-12 10:00:00', 'org-159')`)
	exec(`INSERT INTO cicd_metrics_daily (repo_id, day, computed_at, org_id, retracted)
VALUES (toUUID('14016000-0000-4000-8000-0000000001b1'), '2026-08-10', '2026-08-12 11:00:00', 'org-159', 1)`)
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", cicdRetractedVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 159")
	}

	body, err := os.ReadFile("rollback/159_cicd_metrics_daily_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if got := cicdRetractedColumn(ctx, t, conn); got != "" {
		t.Fatalf("after the down file the retracted column is still %q", got)
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 159")
	}

	var pipelines float64
	var keys uint64
	if err := conn.QueryRow(ctx, cicdRetractedImageRead).Scan(&pipelines, &keys); err != nil {
		t.Fatalf("the read of the build before 159 fails on the rolled-back table: %v", err)
	}
	if pipelines != 0 || keys != 1 {
		t.Fatalf("the older read gives %v pipelines over %d keys, want the marker row as a stored row of zeros (0 over 1)", pipelines, keys)
	}
	exec(cicdRetractedImageInsert)
	if err := conn.QueryRow(ctx, cicdRetractedImageRead).Scan(&pipelines, &keys); err != nil {
		t.Fatal(err)
	}
	if pipelines != 2 || keys != 2 {
		t.Fatalf("after the older writer's row the older read gives %v pipelines over %d keys, want 2 over 2", pipelines, keys)
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != cicdRetractedVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 159 applied", result, err)
	}
	if got := cicdRetractedColumn(ctx, t, conn); got != "UInt8 DEFAULT 0" {
		t.Fatalf("after the upgrade again the retracted column is %q", got)
	}
	var live uint64
	if err := conn.QueryRow(ctx, "SELECT countIf(retracted = 0) FROM cicd_metrics_daily WHERE org_id = 'org-159'").Scan(&live); err != nil || live != 3 {
		t.Fatalf("after the upgrade again %d rows read retracted = 0 (err %v), want all 3 stored rows (the marker row is a stored zero row now)", live, err)
	}
}
