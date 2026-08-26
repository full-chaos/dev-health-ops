//go:build integration

package report

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestClickHouseQueryAdapterRecomputesTeamMetricsDailyRatioAcrossRepos is
// the "no regression" red-first proof for CHAOS-4329's report-engine
// finding (codex round 2): before CHAOS-4329, team_metrics_daily had no
// repo_id, so its dedup source yielded exactly one row per (team, day) and
// a plain avg(after_hours_commit_ratio) was trivially correct (an average
// over one value). After CHAOS-4329, a team owning N repos yields N rows
// per (team, day) -- an unweighted avg() over those rows is WRONG the
// moment repos have different sizes/ratios, which this test's fixture
// deliberately exercises: repo-a (8 commits, 2 after-hours, ratio 0.25)
// and repo-b (2 commits, 2 after-hours, ratio 1.0) would average to 0.625,
// but the TRUE team-day ratio (summed counts, ratio recomputed) is
// 4/10 = 0.4. buildChartQuery's numerator/denominator recompute (declared
// table-locally in metric_registry.json for team_metrics_daily's two
// ratio metrics) must return 0.4, not 0.625.
func TestClickHouseQueryAdapterRecomputesTeamMetricsDailyRatioAcrossRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Exact production schema (001_metrics_v2.sql + 024_add_org_id.sql +
	// 080_team_metrics_daily_repo_id.sql -- CHAOS-4329/CHAOS-4332).
	if err := conn.Exec(ctx, `
CREATE TABLE team_metrics_daily (
    day Date, team_id LowCardinality(String), team_name String,
    commits_count UInt32, after_hours_commits_count UInt32, weekend_commits_count UInt32,
    after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC'), org_id String, repo_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, team_id, day)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO team_metrics_daily
    (day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
     after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
VALUES
    ('2026-01-01', 'core', 'Core', 8, 2, 0, 0.25, 0.0, '2026-01-02 00:00:00.000000', 'org-1', 'repo-a'),
    ('2026-01-01', 'core', 'Core', 2, 2, 0, 1.0,  0.0, '2026-01-02 00:00:01.000000', 'org-1', 'repo-b')`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "after_hours_commit_ratio", GroupBy: "day", FilterTeams: []string{"core"},
				TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-01",
				OrganizationID: "org-1",
			}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
		t.Fatalf("chart result = %#v", result.Charts)
	}
	got := result.Charts[0].DataPoints[0].Y
	if got < 0.399 || got > 0.401 {
		t.Fatalf(
			"after_hours_commit_ratio y=%v, want ~0.4 (sum(after_hours)/sum(commits) = 4/10) -- "+
				"0.625 would mean an unweighted avg() over the two repos' ratios (the regression this test guards)",
			got,
		)
	}
}

// TestClickHouseQueryAdapterOrgWideRatioAveragesTeamsNotRepos is the
// codex round 2 [P1] proof: "preserve team-level averaging for report
// ratios". An org-wide chart (no team filter) must keep averaging EACH
// TEAM's own ratio equally -- summing numerator/denominator across TEAMS
// (not just repos within a team) would silently change this chart's
// existing equal-weighted "avg across teams" semantics into a
// commit-weighted ratio across the whole org. team-a (1 commit, 1
// after-hours = ratio 1.0) and team-b (99 commits, 0 after-hours = ratio
// 0.0): the correct equal-weighted average is 0.5; a commit-weighted sum
// would read 0.01 -- the regression this test guards.
func TestClickHouseQueryAdapterOrgWideRatioAveragesTeamsNotRepos(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE team_metrics_daily (
    day Date, team_id LowCardinality(String), team_name String,
    commits_count UInt32, after_hours_commits_count UInt32, weekend_commits_count UInt32,
    after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC'), org_id String, repo_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, team_id, day)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO team_metrics_daily
    (day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
     after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
VALUES
    ('2026-01-01', 'team-a', 'A', 1,  1, 0, 1.0, 0.0, '2026-01-02 00:00:00.000000', 'org-1', 'repo-a'),
    ('2026-01-01', 'team-b', 'B', 99, 0, 0, 0.0, 0.0, '2026-01-02 00:00:00.000000', 'org-1', 'repo-b')`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "after_hours_commit_ratio", GroupBy: "day", // no FilterTeams -- org-wide
				TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-01",
				OrganizationID: "org-1",
			}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
		t.Fatalf("chart result = %#v", result.Charts)
	}
	got := result.Charts[0].DataPoints[0].Y
	if got < 0.499 || got > 0.501 {
		t.Fatalf(
			"after_hours_commit_ratio y=%v, want ~0.5 (equal-weighted average of team-a's 1.0 and team-b's 0.0) -- "+
				"0.01 would mean numerator/denominator were summed across TEAMS instead of just repos within each team",
			got,
		)
	}
}

// TestClickHouseQueryAdapterDropsLegacyBucketOnceARealPerRepoBackfillExists
// is the codex round 3 [P1] proof: a historical day that has BOTH the
// migration's legacy empty-string repo_id aggregate AND a later real
// per-repo backfill for the same (team, day) must not sum the two together -- that
// would double-count the day. Legacy row: 4 commits/2 after-hours (ratio
// 0.5). Real backfill: repo-a 8/2 (0.25) -- the only repo, so the correct
// team-day ratio is 0.25, not the double-counted (4+8)/(2+2)=0.333 an
// unfiltered SUM would produce.
func TestClickHouseQueryAdapterDropsLegacyBucketOnceARealPerRepoBackfillExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close ClickHouse: %v", err)
		}
	}()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, `
CREATE TABLE team_metrics_daily (
    day Date, team_id LowCardinality(String), team_name String,
    commits_count UInt32, after_hours_commits_count UInt32, weekend_commits_count UInt32,
    after_hours_commit_ratio Float64, weekend_commit_ratio Float64,
    computed_at DateTime64(6, 'UTC'), org_id String, repo_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, team_id, day)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO team_metrics_daily
    (day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
     after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
VALUES
    ('2026-01-01', 'core', 'Core', 4, 2, 0, 0.5,  0.0, '2026-01-01 12:00:00.000000', 'org-1', ''),
    ('2026-01-01', 'core', 'Core', 8, 2, 0, 0.25, 0.0, '2026-01-02 00:00:00.000000', 'org-1', 'repo-a')`); err != nil {
		t.Fatal(err)
	}
	loader := reportLoaderFunc(func(context.Context, QueryInput) (ReportDefinition, error) {
		return ReportDefinition{
			Plan: Plan{PlanID: "plan-1", ReportType: "weekly_health", OrganizationID: "org-1"},
			Charts: []ChartSpec{{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "after_hours_commit_ratio", GroupBy: "day", FilterTeams: []string{"core"},
				TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-01",
				OrganizationID: "org-1",
			}},
		}, nil
	})
	adapter, err := NewClickHouseQueryAdapter(loader, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Query(ctx, QueryInput{ReportID: "report-1", RunID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Charts) != 1 || len(result.Charts[0].DataPoints) != 1 {
		t.Fatalf("chart result = %#v", result.Charts)
	}
	got := result.Charts[0].DataPoints[0].Y
	if got < 0.249 || got > 0.251 {
		t.Fatalf(
			"after_hours_commit_ratio y=%v, want ~0.25 (repo-a's real backfill alone) -- "+
				"0.333 would mean the legacy repo_id='' bucket was summed together with the real per-repo backfill",
			got,
		)
	}
}
