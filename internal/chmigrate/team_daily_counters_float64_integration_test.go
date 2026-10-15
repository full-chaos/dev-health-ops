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

// CHAOS-9601: migration 124 widens the counters of team_complexity_daily and
// team_metrics_daily, and the review-window counts of compounding_risk_daily, to Float64 and rollback/124_*.down.sql narrows them. The
// test rehearses both on the pre-124 shape: whole numbers convert exactly in
// both directions, a fraction written after the upgrade survives until the
// downgrade and is truncated toward zero by it, and the upgrade runs again.
func TestTeamDailyCountersMigrationUpgradesAndDowngrades(t *testing.T) {
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
	columnType := func(table, column string) string {
		t.Helper()
		var out string
		if err := conn.QueryRow(ctx, "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?", table, column).Scan(&out); err != nil {
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

	exec(`CREATE TABLE team_complexity_daily (
    org_id String, team_id String, day Date, loc_total UInt64, cyclomatic_total UInt64,
    cyclomatic_per_kloc Float64, high_complexity_functions UInt64, very_high_complexity_functions UInt64,
    contributing_repo_count UInt32, computed_at DateTime64(6, 'UTC')
) ENGINE = MergeTree ORDER BY (org_id, team_id, day)`)
	exec(`CREATE TABLE team_metrics_daily (
    day Date, team_id String, team_name String, commits_count UInt32, after_hours_commits_count UInt32,
    weekend_commits_count UInt32, after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC'), org_id String, repo_id String
) ENGINE = MergeTree ORDER BY (team_id, day)`)
	exec(`CREATE TABLE compounding_risk_daily (
    org_id String, day Date, scope String, scope_id String, compounding_risk Nullable(Float64),
    merged_prs_30d Nullable(UInt32), reviewed_prs_30d Nullable(UInt32), review_share_30d Nullable(Float64)
) ENGINE = MergeTree ORDER BY (org_id, scope, scope_id, day)`)
	exec(`INSERT INTO compounding_risk_daily VALUES ('o', '2026-08-01', 'team', 't', 0.5, 10, 6, 0.6)`)
	exec(`INSERT INTO team_complexity_daily VALUES ('o', 't', '2026-08-01', 2000, 100, 50, 3, 1, 1, now64(6))`)
	exec(`INSERT INTO team_metrics_daily VALUES ('2026-08-01', 't', 'T', 20, 4, 2, 0.2, 0.1, now64(6), 'o', 'r')`)

	const up = "sql/124_team_daily_counters_float64.sql"
	const down = "rollback/124_team_daily_counters_float64.down.sql"
	versionRows := useSchemaMigrations(t, ctx, conn, "124_team_daily_counters_float64.sql")
	run(up)
	for _, column := range [][2]string{
		{"team_complexity_daily", "loc_total"}, {"team_complexity_daily", "cyclomatic_total"},
		{"team_complexity_daily", "high_complexity_functions"}, {"team_complexity_daily", "very_high_complexity_functions"},
		{"team_metrics_daily", "commits_count"}, {"team_metrics_daily", "after_hours_commits_count"}, {"team_metrics_daily", "weekend_commits_count"},
	} {
		if got := columnType(column[0], column[1]); got != "Float64" {
			t.Errorf("after the upgrade %s.%s is %s, want Float64", column[0], column[1], got)
		}
	}
	for _, column := range []string{"merged_prs_30d", "reviewed_prs_30d"} {
		if got := columnType("compounding_risk_daily", column); got != "Nullable(Float64)" {
			t.Errorf("after the upgrade compounding_risk_daily.%s is %s, want Nullable(Float64)", column, got)
		}
	}
	if got := readFloat(`SELECT loc_total FROM team_complexity_daily`); got != 2000 {
		t.Errorf("a whole count after the upgrade = %v, want 2000 exactly", got)
	}
	if got := readFloat(`SELECT merged_prs_30d FROM compounding_risk_daily`); got != 10 {
		t.Errorf("a whole review-window count after the upgrade = %v, want 10 exactly", got)
	}
	exec(`INSERT INTO team_complexity_daily VALUES ('o', 'lesser', '2026-08-01', 800.5, 40.25, 50, 1.8, 0.4, 1, now64(6))`)
	exec(`INSERT INTO team_metrics_daily VALUES ('2026-08-01', 'lesser', 'L', 8.5, 1.6, 0.4, 0.2, 0.05, now64(6), 'o', 'r')`)
	exec(`INSERT INTO compounding_risk_daily VALUES ('o', '2026-08-01', 'team', 'lesser', 0.5, 3.3, 1.8, 0.6)`)
	if got := readFloat(`SELECT loc_total FROM team_complexity_daily WHERE team_id = 'lesser'`); got != 800.5 {
		t.Errorf("a fraction after the upgrade = %v, want 800.5 unrounded", got)
	}

	run(down)
	if n := versionRows(); n != 0 {
		t.Errorf("after the rollback the version row is still recorded (%d rows)", n)
	}
	for _, column := range [][3]string{
		{"team_complexity_daily", "loc_total", "UInt64"}, {"team_complexity_daily", "high_complexity_functions", "UInt64"},
		{"team_metrics_daily", "commits_count", "UInt32"}, {"team_metrics_daily", "weekend_commits_count", "UInt32"},
	} {
		if got := columnType(column[0], column[1]); got != column[2] {
			t.Errorf("after the downgrade %s.%s is %s, want %s", column[0], column[1], got, column[2])
		}
	}
	if got := readFloat(`SELECT loc_total FROM team_complexity_daily WHERE team_id = 't'`); got != 2000 {
		t.Errorf("a whole count after the downgrade = %v, want 2000 exactly", got)
	}
	if got := readFloat(`SELECT commits_count FROM team_metrics_daily WHERE team_id = 't'`); got != 20 {
		t.Errorf("a whole commit count after the downgrade = %v, want 20 exactly", got)
	}
	if got := columnType("compounding_risk_daily", "merged_prs_30d"); got != "Nullable(UInt32)" {
		t.Errorf("after the downgrade compounding_risk_daily.merged_prs_30d is %s, want Nullable(UInt32)", got)
	}
	if got := readFloat(`SELECT merged_prs_30d FROM compounding_risk_daily WHERE scope_id = 't'`); got != 10 {
		t.Errorf("a whole review-window count after the downgrade = %v, want 10 exactly", got)
	}
	if got := readFloat(`SELECT merged_prs_30d FROM compounding_risk_daily WHERE scope_id = 'lesser'`); got != 3 {
		t.Errorf("3.3 merged pull requests after the downgrade = %v, want 3 (truncated)", got)
	}
	// The documented loss: a fraction is truncated toward zero.
	if got := readFloat(`SELECT loc_total FROM team_complexity_daily WHERE team_id = 'lesser'`); got != 800 {
		t.Errorf("800.5 after the downgrade = %v, want 800 (truncated)", got)
	}
	if got := readFloat(`SELECT weekend_commits_count FROM team_metrics_daily WHERE team_id = 'lesser'`); got != 0 {
		t.Errorf("0.4 of a commit after the downgrade = %v, want 0 (truncated)", got)
	}

	run(up)
	if got := columnType("team_metrics_daily", "commits_count"); got != "Float64" {
		t.Errorf("after a second upgrade commits_count is %s, want Float64", got)
	}
}
