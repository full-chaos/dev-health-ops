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

// Migration 135 makes lead_p50_hours and churn_loc Nullable and sets every stored lead time of 0 to NULL (a lead time of
// 0 hours was never stored as a measurement). The test rehearses it on the pre-135 shape with the rows the old writers
// left: the report mean of the lead time counts the measured 34 only after the upgrade (17 before it: the legacy 0 was a
// sample), a stored churn of 0 is left alone, and the rollback gives the 0 back and the upgrade runs again.
func TestWorkItemMetricsNullableMigrationBackfillsTheLeadZeroOnly(t *testing.T) {
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
	scalar := func(query string) float64 {
		t.Helper()
		var out float64
		if err := conn.QueryRow(ctx, "SELECT toFloat64(ifNull(("+query+"), -1))").Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}
	count := func(query string) uint64 {
		t.Helper()
		var out uint64
		if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	exec(`CREATE TABLE issue_type_metrics_daily (
    repo_id Nullable(UUID), day Date, provider LowCardinality(String), team_id LowCardinality(String), issue_type_norm LowCardinality(String),
    created_count UInt32, completed_count UInt32, active_count UInt32, cycle_p50_hours Float64, cycle_p90_hours Float64,
    lead_p50_hours Float64, computed_at DateTime DEFAULT now(), org_id String DEFAULT 'default'
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, day, provider, team_id, issue_type_norm)`)
	exec(`CREATE TABLE work_item_completion_daily (
    org_id String, repo_id Nullable(UUID), day Date, team_id LowCardinality(String), delivery_units UInt32, work_items_completed UInt32,
    prs_merged UInt32, churn_loc UInt64, cycle_p50_hours Float64, computed_at DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, day, team_id) SETTINGS allow_nullable_key = 1`)
	exec(`INSERT INTO issue_type_metrics_daily (day, provider, team_id, issue_type_norm, created_count, completed_count, active_count, cycle_p50_hours, cycle_p90_hours, lead_p50_hours, org_id) VALUES
('2026-08-01', 'github', 'team-a', 'bug', 0, 2, 0, 0, 0, 34, 'o'),
('2026-08-01', 'github', 'team-b', 'bug', 0, 2, 0, 0, 0, 0, 'o'),
('2026-08-01', 'github', 'team-c', 'bug', 0, 0, 0, 0, 0, 0, 'o')`)
	exec(`INSERT INTO work_item_completion_daily (org_id, day, team_id, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours) VALUES
('o', '2026-08-01', 'team-a', 1, 1, 0, 0, 0), ('o', '2026-08-01', 'team-b', 1, 1, 0, 25, 0)`)

	if got := scalar(`SELECT avg(lead_p50_hours) FROM issue_type_metrics_daily WHERE completed_count > 0`); got != 17 {
		t.Fatalf("fixture: the pre-migration mean = %v, want 17 (the legacy 0 counted as a sample)", got)
	}

	const up = "sql/135_work_item_metrics_nullable.sql"
	const down = "rollback/135_work_item_metrics_nullable.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "135_work_item_metrics_nullable.sql")
	run(up)
	if got := count(`SELECT count() FROM issue_type_metrics_daily WHERE lead_p50_hours IS NULL`); got != 2 {
		t.Errorf("after the upgrade %d rows hold a NULL lead time, want 2 (both stored 0)", got)
	}
	if got := scalar(`SELECT avg(lead_p50_hours) FROM issue_type_metrics_daily WHERE completed_count > 0`); got != 34 {
		t.Errorf("the mean lead time after the upgrade = %v, want 34 (the legacy 0 is no longer a sample)", got)
	}
	if got := scalar(`SELECT lead_p50_hours FROM issue_type_metrics_daily WHERE team_id = 'team-a'`); got != 34 {
		t.Errorf("the measured lead time after the upgrade = %v, want 34", got)
	}
	if got := count(`SELECT count() FROM work_item_completion_daily WHERE churn_loc IS NULL`); got != 0 {
		t.Errorf("after the upgrade %d churn rows are NULL, want 0: a stored churn of 0 is not rewritten", got)
	}
	if got := scalar(`SELECT churn_loc FROM work_item_completion_daily WHERE team_id = 'team-a'`); got != 0 {
		t.Errorf("the stored churn of 0 after the upgrade = %v, want 0", got)
	}

	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}
	if got := count(`SELECT count() FROM issue_type_metrics_daily WHERE lead_p50_hours = 0`); got != 2 {
		t.Errorf("after the rollback %d rows hold a lead time of 0, want 2 (the NULL rows read 0 again)", got)
	}

	run(up)
	if got := scalar(`SELECT avg(lead_p50_hours) FROM issue_type_metrics_daily WHERE completed_count > 0`); got != 34 {
		t.Errorf("the mean lead time after a second upgrade = %v, want 34", got)
	}
}
