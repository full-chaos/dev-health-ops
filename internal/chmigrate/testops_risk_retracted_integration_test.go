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

// riskRetractedImageInserts are the writes of the build before 158 (writeTestopsRisk): they name their columns and never
// name retracted.
var riskRetractedImageInserts = map[string]string{
	"testops_release_confidence": `INSERT INTO testops_release_confidence (repo_id, day, confidence_score, pipeline_success_factor,
  test_pass_factor, coverage_factor, flake_penalty, regression_penalty, factors_json, team_id, service_id, org_id, computed_at)
VALUES (toUUID('14009000-0000-4000-8000-0000000001b1'), '2026-08-11', 0.5, 0.5, 0.5, 0.5, 0, 0, '{}', 'team-1', 'svc-1', 'org-158', '2026-08-13 10:00:00')`,
	"testops_quality_drag": `INSERT INTO testops_quality_drag (repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours,
  queue_wait_hours, retry_overhead_hours, factors_json, team_id, service_id, org_id, computed_at)
VALUES (toUUID('14009000-0000-4000-8000-0000000001b1'), '2026-08-11', 0.5, 0.5, 0, 0, 0, '{}', 'team-1', 'svc-1', 'org-158', '2026-08-13 10:00:00')`,
	"testops_pipeline_stability": `INSERT INTO testops_pipeline_stability (repo_id, day, stability_index, success_rate_7d, success_rate_trend,
  failure_clustering_score, median_recovery_time_seconds, team_id, service_id, org_id, computed_at)
VALUES (toUUID('14009000-0000-4000-8000-0000000001b1'), '2026-08-11', 0.5, 0.5, 0, 0, NULL, 'team-1', 'svc-1', 'org-158', '2026-08-13 10:00:00')`,
}

// riskRetractedImageRead is the newest-row read of the build before 158 (testopsrisk dailyQuery: argMax by computed_at
// per repository day), counted.
func riskRetractedImageRead(table string) string {
	return `SELECT count() FROM (SELECT day, repo_id, max(computed_at) FROM ` + table + `
WHERE org_id = 'org-158' AND day >= '2026-08-10' AND day <= '2026-08-11' GROUP BY day, repo_id)`
}

func riskRetractedColumns(ctx context.Context, t *testing.T, conn driver.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT table, type || ' ' || default_kind || ' ' || default_expression FROM system.columns
WHERE database = currentDatabase() AND table IN ('testops_release_confidence', 'testops_quality_drag', 'testops_pipeline_stability')
  AND name = 'retracted'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := map[string]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatal(err)
		}
		columns[table] = column
	}
	return columns
}

// INVARIANT (CHAOS-14009): the rollback of 158 is "images back on a build without 158, then 158's down file". Up, down, up on
// a real ClickHouse: the columns are gone after the down file and its version row too; the read and the INSERT of the build
// before 158 work on the tables in that shape (a marker row reads as a stored row of zeros, as the down file says); the upgrade
// applies 158 again.
func TestRollback158UpDownUp(t *testing.T) {
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
	expectColumns := func(when string) {
		t.Helper()
		columns := riskRetractedColumns(ctx, t, conn)
		for _, table := range testopsRiskRetractedTables {
			if got := columns[table]; got != "UInt8 DEFAULT 0" {
				t.Fatalf("%s the retracted column of %s is %q, want UInt8 DEFAULT 0", when, table, got)
			}
		}
	}
	expectColumns("at the head")
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%v\n%s", err, statement)
		}
	}
	for _, table := range testopsRiskRetractedTables {
		exec(`INSERT INTO ` + table + ` (repo_id, day, team_id, service_id, org_id, computed_at)
VALUES (toUUID('14009000-0000-4000-8000-0000000001b1'), '2026-08-10', 'team-1', 'svc-1', 'org-158', '2026-08-12 10:00:00')`)
		exec(`INSERT INTO ` + table + ` (repo_id, day, team_id, service_id, org_id, computed_at, retracted)
VALUES (toUUID('14009000-0000-4000-8000-0000000001b1'), '2026-08-10', 'team-1', 'svc-1', 'org-158', '2026-08-12 11:00:00', 1)`)
	}
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", testopsRiskRetractedVersion).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if versionRows() == 0 {
		t.Fatalf("at the head schema_migrations holds no %s", testopsRiskRetractedVersion)
	}

	body, err := os.ReadFile("rollback/158_testops_risk_retracted.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		exec(statement)
	}
	if columns := riskRetractedColumns(ctx, t, conn); len(columns) != 0 {
		t.Fatalf("after the down file the retracted columns are still %v", columns)
	}
	if versionRows() != 0 {
		t.Fatalf("after the down file schema_migrations still holds %s", testopsRiskRetractedVersion)
	}

	for _, table := range testopsRiskRetractedTables {
		var keys uint64
		if err := conn.QueryRow(ctx, riskRetractedImageRead(table)).Scan(&keys); err != nil {
			t.Fatalf("the read of the build before 158 fails on the rolled-back %s: %v", table, err)
		}
		if keys != 1 {
			t.Fatalf("the older read of %s gives %d keys, want the marker row as one stored row of zeros", table, keys)
		}
		exec(riskRetractedImageInserts[table])
		if err := conn.QueryRow(ctx, riskRetractedImageRead(table)).Scan(&keys); err != nil {
			t.Fatal(err)
		}
		if keys != 2 {
			t.Fatalf("after the older writer's row the older read of %s gives %d keys, want 2", table, keys)
		}
	}

	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil || len(result.Applied) != 1 || result.Applied[0] != testopsRiskRetractedVersion {
		t.Fatalf("the upgrade after the rollback = %+v, %v; want %s applied", result, err, testopsRiskRetractedVersion)
	}
	expectColumns("after the upgrade again")
	for _, table := range testopsRiskRetractedTables {
		var live uint64
		if err := conn.QueryRow(ctx, "SELECT countIf(retracted = 0) FROM "+table+" WHERE org_id = 'org-158'").Scan(&live); err != nil || live != 3 {
			t.Fatalf("after the upgrade again %d rows of %s read retracted = 0 (err %v), want all 3 stored rows", live, table, err)
		}
	}
}
