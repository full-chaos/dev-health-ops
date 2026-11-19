//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// deployImageInsert is the deploy write of the build before 163 (deploy_native_clickhouse.go WriteDeployMetricsDaily): it names
// its columns and never names retracted.
const deployImageInsert = `INSERT INTO deploy_metrics_daily (repo_id, day, deployments_count, failed_deployments_count,
  deploy_time_p50_hours, lead_time_p50_hours, computed_at, org_id, pooled_deploy_hours, pooled_samples_stored)
VALUES ('16300000-0000-4000-8000-000000000001', '2026-09-10', 2, 0, 3, NULL, '2026-09-11 10:00:00', 'org-163', [1, 5], 1)`

// The rollback of 163 is "query-api and the worker image back on a build without 163, then 163's down file". Up, down, up on a
// real ClickHouse: the column of each of the four tables is gone after the down file and its version row too; the write of the
// build before 163 works on the table in that shape; the upgrade applies 163 again and every stored row reads 0.
func TestRollback163UpDownUp(t *testing.T) {
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
	for _, table := range dailyPooledRetractedTables {
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
	exec(deployImageInsert)
	exec(`INSERT INTO deploy_metrics_daily (repo_id, day, org_id, retracted, computed_at)
VALUES ('16300000-0000-4000-8000-000000000001', '2026-09-10', 'org-163', 1, '2026-09-11 11:00:00')`)
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", dailyPooledRetractedVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatal("at the head schema_migrations holds no 163")
	}

	body, err := os.ReadFile("rollback/163_daily_pooled_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	for _, table := range dailyPooledRetractedTables {
		if got := benchmarkingRetractedColumn(ctx, t, conn, table); got != "" {
			t.Fatalf("after the down file the retracted column of %s is still %q", table, got)
		}
	}
	if versionRows() != 0 {
		t.Fatal("after the down file schema_migrations still holds 163")
	}
	exec(deployImageInsert)

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != dailyPooledRetractedVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want 163 applied", result, err)
	}
	var stored, live uint64
	if err := conn.QueryRow(ctx, "SELECT count(), countIf(retracted = 0) FROM deploy_metrics_daily WHERE org_id = 'org-163'").
		Scan(&stored, &live); err != nil || stored != 3 || live != 3 {
		t.Fatalf("after the upgrade again %d of %d rows read retracted = 0 (err %v), want all 3", live, stored, err)
	}
}
