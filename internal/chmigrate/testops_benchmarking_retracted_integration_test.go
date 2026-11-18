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

var benchmarkingRetractedTables = []string{
	"testops_metric_baselines", "testops_maturity_bands", "testops_metric_anomalies",
	"testops_period_comparisons", "testops_metric_correlations", "testops_benchmark_insights",
}

// benchmarkingImageInsert is the maturity-band write of the build before 156 (benchmarking/clickhouse.go writeMaturityBands):
// it names its columns and never names retracted.
const benchmarkingImageInsert = `INSERT INTO testops_maturity_bands (metric_name, scope_type, scope_key, period_start, period_end,
  value, percentile_rank, maturity_band, confidence, org_id, computed_at)
VALUES ('pass_rate', 'repo', 'repo-156', '2026-08-12', '2026-09-10', 0.9, 0.5, 'developing', 0.8, 'org-156', '2026-09-11 10:00:00')`

func benchmarkingRetractedColumn(ctx context.Context, t *testing.T, conn driver.Conn, table string) string {
	t.Helper()
	var column string
	rows, err := conn.Query(ctx, `SELECT type || ' ' || default_kind || ' ' || default_expression FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name = 'retracted'`, table)
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

// The rollback of 156 is "the worker image back on a build without 156, then 156's down file". Up, down, up on a real
// ClickHouse: the column of each of the six tables is gone after the down file and its version row too; the write of the
// build before 156 works on the table in that shape; the upgrade applies 156 again and a stored row reads 0.
func TestRollback156UpDownUp(t *testing.T) {
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
	for _, table := range benchmarkingRetractedTables {
		if got := benchmarkingRetractedColumn(ctx, t, conn, table); got != "UInt8 DEFAULT 0" {
			t.Fatalf("at the head the retracted column of %s is %q, want UInt8 DEFAULT 0", table, got)
		}
	}
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	exec(benchmarkingImageInsert)
	exec(`INSERT INTO testops_maturity_bands (metric_name, scope_type, scope_key, period_end, org_id, retracted, computed_at)
VALUES ('pass_rate', 'repo', 'repo-156', '2026-09-10', 'org-156', 1, '2026-09-11 11:00:00')`)
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", benchmarkingRetractedVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 156")
	}

	body, err := os.ReadFile("rollback/156_testops_benchmarking_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	for _, table := range benchmarkingRetractedTables {
		if got := benchmarkingRetractedColumn(ctx, t, conn, table); got != "" {
			t.Fatalf("after the down file the retracted column of %s is still %q", table, got)
		}
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 156")
	}
	exec(benchmarkingImageInsert)

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != benchmarkingRetractedVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 156 applied", result, err)
	}
	var stored, live uint64
	if err := conn.QueryRow(ctx, "SELECT count(), countIf(retracted = 0) FROM testops_maturity_bands WHERE org_id = 'org-156'").
		Scan(&stored, &live); err != nil || stored != 3 || live != 3 {
		t.Fatalf("after the upgrade again %d of %d rows read retracted = 0 (err %v), want all 3", live, stored, err)
	}
}
