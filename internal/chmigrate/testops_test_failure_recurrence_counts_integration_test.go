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

// The writer of the build before 169 names its columns and never names the two counts.
const recurrenceCountsImageInsert = `INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count,
  pass_rate, failure_rate, total_suites, failure_recurrence_score, flake_cases, distinct_cases, team_id, computed_at)
VALUES ('org-169', toUUID('14171000-0000-4000-8000-0000000001b2'), '2026-08-11', 2, 1, 1, 0.5, 0.5, 1, 1, 0, 2, 'team-1', '2026-08-13 10:00:00')`

func recurrenceCountColumns(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	columns := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns WHERE database = currentDatabase()
  AND table = 'testops_test_metrics_daily' AND name IN ('recurrent_failed_names', 'distinct_failed_names')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			t.Fatal(err)
		}
		columns[name] = kind
	}
	return columns
}

// Up, down, up on a real ClickHouse: at the head a row stored before 169 reads NULL in both counts; the down file drops both
// columns and the version row, and the INSERT of the build before 169 works on that shape; the upgrade applies 169 again.
func TestRollback169UpDownUp(t *testing.T) {
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
	wantColumns := func(when string) {
		t.Helper()
		got := recurrenceCountColumns(ctx, t, conn)
		if len(got) != 2 || got["recurrent_failed_names"] != "Nullable(UInt32)" || got["distinct_failed_names"] != "Nullable(UInt32)" {
			t.Fatalf("%s the count columns are %v, want both Nullable(UInt32)", when, got)
		}
	}
	wantColumns("at the head")
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(recurrenceCountsImageInsert)
	var withoutCounts uint64
	if err := conn.QueryRow(ctx, `SELECT countIf(recurrent_failed_names IS NULL AND distinct_failed_names IS NULL)
FROM testops_test_metrics_daily WHERE org_id = 'org-169'`).Scan(&withoutCounts); err != nil || withoutCounts != 1 {
		t.Fatalf("a row of the older writer reads %d rows without counts (err %v), want 1", withoutCounts, err)
	}
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", failureRecurrenceCountsVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 169")
	}

	body, err := os.ReadFile("rollback/169_testops_test_failure_recurrence_counts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if got := recurrenceCountColumns(ctx, t, conn); len(got) != 0 {
		t.Fatalf("after the down file the count columns are still %v", got)
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 169")
	}
	exec(recurrenceCountsImageInsert)

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != failureRecurrenceCountsVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 169 applied", result, err)
	}
	wantColumns("after the upgrade again")
	var rows uint64
	if err := conn.QueryRow(ctx, `SELECT countIf(recurrent_failed_names IS NULL AND distinct_failed_names IS NULL)
FROM testops_test_metrics_daily WHERE org_id = 'org-169'`).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("after the upgrade again %d rows read no counts (err %v), want both stored rows", rows, err)
	}
}
