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

var aiImpactCountColumns = []string{
	"prs_total", "prs_merged", "ai_assisted_prs", "agent_created_prs", "human_prs", "unknown_prs",
	"agent_created_pr_count", "rework_prs", "followup_commits_count", "revert_prs", "incidents_count", "test_gap_prs",
}

// CHAOS-9604: migration 128 widens the twelve count columns of
// ai_impact_metrics_daily to Float64 and rollback/128_*.down.sql narrows them.
// The test rehearses both on the pre-128 shape: whole counts convert exactly in
// both directions, a fraction written after the upgrade survives until the
// downgrade and is truncated toward zero by it, and the upgrade runs again.
func TestAIImpactCountsMigrationUpgradesAndDowngrades(t *testing.T) {
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
	columnType := func(column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ai_impact_metrics_daily' AND name = ?", column).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	readFloat := func(query string) float64 {
		t.Helper()
		var out float64
		if err := conn.QueryRow(ctx, "SELECT toFloat64(("+query+"))").Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	exec(`CREATE TABLE ai_impact_metrics_daily (
    org_id String, team_id String, repo_id UUID, work_type LowCardinality(String), day Date,
    attribution_bucket LowCardinality(String),
    prs_total UInt32, prs_merged UInt32, ai_assisted_prs UInt32, agent_created_prs UInt32,
    human_prs UInt32, unknown_prs UInt32, ai_assisted_pr_ratio Nullable(Float64), agent_created_pr_count UInt32,
    rework_prs UInt32, rework_drag_rate Nullable(Float64), followup_commits_count UInt32,
    revert_prs UInt32, incidents_count UInt32, test_gap_prs UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day)
ORDER BY (org_id, team_id, repo_id, work_type, day, attribution_bucket)`)
	exec(`INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
    prs_total, prs_merged, ai_assisted_prs, agent_created_prs, human_prs, unknown_prs, agent_created_pr_count,
    rework_prs, followup_commits_count, revert_prs, incidents_count, test_gap_prs)
VALUES ('o', 't', generateUUIDv4(), 'pull_request', '2026-08-01', 'human', 10, 8, 3, 2, 4, 1, 2, 5, 7, 1, 2, 4)`)

	const up = "sql/128_ai_impact_counts_float64.sql"
	const down = "rollback/128_ai_impact_counts_float64.down.sql"
	run(up)
	for _, column := range aiImpactCountColumns {
		if got := columnType(column); got != "Float64" {
			t.Errorf("after the upgrade ai_impact_metrics_daily.%s is %s, want Float64", column, got)
		}
	}
	if got := readFloat(`SELECT prs_total FROM ai_impact_metrics_daily WHERE team_id = 't'`); got != 10 {
		t.Errorf("a whole count after the upgrade = %v, want 10 exactly", got)
	}
	exec(`INSERT INTO ai_impact_metrics_daily (org_id, team_id, repo_id, work_type, day, attribution_bucket,
    prs_total, prs_merged, ai_assisted_prs, agent_created_prs, human_prs, unknown_prs, agent_created_pr_count,
    rework_prs, followup_commits_count, revert_prs, incidents_count, test_gap_prs)
VALUES ('o', 'lesser', generateUUIDv4(), 'pull_request', '2026-08-01', 'human', 3.3, 2.4, 0.9, 0.6, 1.2, 0.3, 0.6, 1.5, 2.1, 0.3, 0.6, 1.2)`)
	if got := readFloat(`SELECT prs_total FROM ai_impact_metrics_daily WHERE team_id = 'lesser'`); got != 3.3 {
		t.Errorf("a fraction after the upgrade = %v, want 3.3 unrounded", got)
	}

	run(down)
	for _, column := range aiImpactCountColumns {
		if got := columnType(column); got != "UInt32" {
			t.Errorf("after the downgrade ai_impact_metrics_daily.%s is %s, want UInt32", column, got)
		}
	}
	if got := readFloat(`SELECT prs_total FROM ai_impact_metrics_daily WHERE team_id = 't'`); got != 10 {
		t.Errorf("a whole count after the downgrade = %v, want 10 exactly", got)
	}
	// The documented loss: a fraction is truncated toward zero.
	if got := readFloat(`SELECT prs_total FROM ai_impact_metrics_daily WHERE team_id = 'lesser'`); got != 3 {
		t.Errorf("3.3 after the downgrade = %v, want 3 (truncated)", got)
	}
	if got := readFloat(`SELECT unknown_prs FROM ai_impact_metrics_daily WHERE team_id = 'lesser'`); got != 0 {
		t.Errorf("0.3 of a pull request after the downgrade = %v, want 0 (truncated)", got)
	}

	run(up)
	if got := columnType("prs_total"); got != "Float64" {
		t.Errorf("after a second upgrade prs_total is %s, want Float64", got)
	}
}
