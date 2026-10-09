package report

import (
	"strings"
	"testing"
)

// TestBuildChartQueryDedupsCicdMetricsDaily is the CHAOS-4246 regression
// guard for the Go weekly-report engine. Before this change buildChartQuery
// read cicd_metrics_daily (and deploy_metrics_daily, incident_metrics_daily)
// raw and aggregated with a bare avg()/sum() -- correct only as long as a day
// was ever computed exactly once. CHAOS-4246's native_post_sync.go fix makes
// a legitimate re-drive of a day common (a cicd/deployments/incidents sync
// now re-triggers metrics.daily_partition for a day whose partition already
// ran), so this reader would sum two generations of the same (org, repo,
// day) instead of reading the latest one.
func TestBuildChartQueryDedupsCicdMetricsDaily(t *testing.T) {
	t.Parallel()
	spec := ChartSpec{
		ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
		Metric: "avg_duration_minutes", GroupBy: "day",
		TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
		OrganizationID: "org-1",
	}
	definition, ok := supportedMetrics[spec.Metric]
	if !ok || definition.SourceTable != "cicd_metrics_daily" {
		t.Fatalf("fixture drift: avg_duration_minutes no longer maps to cicd_metrics_daily (got %+v)", definition)
	}
	query, _, err := buildChartQuery(spec, definition)
	if err != nil {
		t.Fatal(err)
	}
	normalized := strings.Join(strings.Fields(query), " ")
	const wantSubquery = "(SELECT * FROM cicd_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS cicd_metrics_daily"
	if !strings.Contains(normalized, wantSubquery) {
		t.Fatalf("query does not dedup cicd_metrics_daily to the latest generation:\n%s", query)
	}
	// A bare, un-deduped read must NOT be reachable via this metric anymore.
	if strings.Contains(normalized, "FROM cicd_metrics_daily WHERE") {
		t.Fatalf("query still reads cicd_metrics_daily raw:\n%s", query)
	}
}

// TestBuildChartQueryDedupsDoraMetricsDaily is the CHAOS-4140 regression
// guard: before this change, dora_metrics_daily was entirely absent from
// appendOnlyDailyKeys, so buildChartQuery read it raw for the "value" chart
// metric (metric_registry.json's only dora entry) and aggregated with a bare
// avg()/sum(). A DORA partition retry writes a fresh computed_at generation
// for every (org_id, repo_id, day, metric_name) it recomputes -- job_dora.py
// and dora_native.go never delete the prior generation, by design (CHAOS-4130
// disposition) -- so this reader summed every generation together, inflating
// every DORA chart (deployment frequency, lead time, MTTR, change failure
// rate) by however many times a partition had been retried.
func TestBuildChartQueryDedupsDoraMetricsDaily(t *testing.T) {
	t.Parallel()
	spec := ChartSpec{
		ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
		Metric: "value", GroupBy: "day",
		TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
		OrganizationID: "org-1",
	}
	definition, ok := supportedMetrics[spec.Metric]
	if !ok || definition.SourceTable != "dora_metrics_daily" {
		t.Fatalf("fixture drift: value no longer maps to dora_metrics_daily (got %+v)", definition)
	}
	query, _, err := buildChartQuery(spec, definition)
	if err != nil {
		t.Fatal(err)
	}
	normalized := strings.Join(strings.Fields(query), " ")
	const wantSubquery = "(SELECT * FROM dora_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, metric_name) AS dora_metrics_daily"
	if !strings.Contains(normalized, wantSubquery) {
		t.Fatalf("query does not dedup dora_metrics_daily to the latest generation:\n%s", query)
	}
	// A bare, un-deduped read must NOT be reachable via this metric anymore.
	if strings.Contains(normalized, "FROM dora_metrics_daily WHERE") {
		t.Fatalf("query still reads dora_metrics_daily raw:\n%s", query)
	}
}

// TestDedupFromSourceEveryAppendOnlyTableAndEveryReplacingTable is
// table-driven and clause-isolated (AGENTS.md mutation-testing note): each
// case names one table so a mutant that drops a single map entry, or
// confuses the ReplacingMergeTree/append-only branches, is caught by the
// case that names exactly that table.
func TestDedupFromSourceEveryAppendOnlyTableAndEveryReplacingTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		table string
		want  string
	}{
		{"work_item_metrics_daily", "work_item_metrics_daily FINAL"},
		{"work_item_user_metrics_daily", "work_item_user_metrics_daily FINAL"},
		{"cicd_metrics_daily", "(SELECT * FROM cicd_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS cicd_metrics_daily"},
		{"deploy_metrics_daily", "(SELECT * FROM deploy_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS deploy_metrics_daily"},
		{"incident_metrics_daily", "(SELECT * FROM incident_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS incident_metrics_daily"},
		{"testops_release_confidence", "(SELECT * FROM testops_release_confidence ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_release_confidence"},
		{"testops_pipeline_stability", "(SELECT * FROM testops_pipeline_stability ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_pipeline_stability"},
		{"testops_quality_drag", "(SELECT * FROM testops_quality_drag ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_quality_drag"},
		{"repo_metrics_daily", "(SELECT * FROM repo_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS repo_metrics_daily"},
		{"team_metrics_daily", "(SELECT * FROM team_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, team_id, repo_id, day) AS team_metrics_daily"},
		{"user_metrics_daily", "(SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day) AS user_metrics_daily"},
		{"testops_pipeline_metrics_daily", "(SELECT * FROM testops_pipeline_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_pipeline_metrics_daily"},
		{"testops_test_metrics_daily", "(SELECT * FROM testops_test_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_test_metrics_daily"},
		{"testops_coverage_metrics_daily", "(SELECT * FROM testops_coverage_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day) AS testops_coverage_metrics_daily"},
		{"dora_metrics_daily", "(SELECT * FROM dora_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, metric_name) AS dora_metrics_daily"},
		// CHAOS-4459 (codex review rounds 2-3): both now registered.
		{"file_metrics_daily", "(SELECT * FROM file_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, path) AS file_metrics_daily"},
		{"file_hotspot_daily", "(SELECT * FROM file_hotspot_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, file_path) AS file_hotspot_daily"},
		// CHAOS-4459 (codex review round 4, key corrected round 5 -- org_id
		// added, migration 024 added the column after 004 created it
		// without one).
		{"review_edges_daily", "(SELECT * FROM review_edges_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, reviewer, author, day) AS review_edges_daily"},
		// CHAOS-4459 (self-audit, requested by team-lead; key corrected
		// round 5 -- org_id added, same migration-024 root cause).
		{"commit_metrics", "(SELECT * FROM commit_metrics ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, author_email, commit_hash) AS commit_metrics"},
	}
	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			t.Parallel()
			if got := dedupFromSource(tc.table); got != tc.want {
				t.Fatalf("dedupFromSource(%q) = %q, want %q", tc.table, got, tc.want)
			}
		})
	}
}

// TestBuildChartQueryReadsTheWorkItemDailyTablesByNewestRow holds the two
// tables the daily families work_item_issue_type and work_item_investment
// append to: every chart metric of metric_registry.json that reads one of
// them reads the newest row of each key, never the raw table.
func TestBuildChartQueryReadsTheWorkItemDailyTablesByNewestRow(t *testing.T) {
	t.Parallel()
	wantSource := map[string]string{
		"issue_type_metrics_daily": "(SELECT * FROM issue_type_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, provider, team_id, issue_type_norm) AS issue_type_metrics_daily",
		"investment_metrics_daily": "(SELECT * FROM investment_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, day, team_id, investment_area, project_stream) AS investment_metrics_daily",
	}
	charted := map[string]int{}
	for name, definition := range supportedMetrics {
		want, ok := wantSource[definition.SourceTable]
		if !ok {
			continue
		}
		charted[definition.SourceTable]++
		query, _, err := buildChartQuery(ChartSpec{
			ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: name, GroupBy: "day",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
		}, definition)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		normalized := strings.Join(strings.Fields(query), " ")
		if !strings.Contains(normalized, "FROM "+want+" WHERE") {
			t.Errorf("%s does not read the newest row of each %s key:\n%s", name, definition.SourceTable, query)
		}
	}
	for table := range wantSource {
		if charted[table] == 0 {
			t.Errorf("fixture drift: no chart metric reads %s any more", table)
		}
	}
}

// TestBuildChartQueryAveragesLeadTimeOverRowsWithACompletedItem holds the
// mean of lead_p50_hours: a row with completed_count = 0 has no lead time and
// is not part of the mean. A count metric of the same table stays a plain sum.
func TestBuildChartQueryAveragesLeadTimeOverRowsWithACompletedItem(t *testing.T) {
	t.Parallel()
	build := func(metric string) string {
		t.Helper()
		definition, ok := supportedMetrics[metric]
		if !ok || definition.SourceTable != "issue_type_metrics_daily" {
			t.Fatalf("fixture drift: %s no longer maps to issue_type_metrics_daily (got %+v)", metric, definition)
		}
		query, _, err := buildChartQuery(ChartSpec{
			ChartID: "chart-1", PlanID: "plan-1", ChartType: "line", Metric: metric, GroupBy: "day",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
		}, definition)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(strings.Fields(query), " ")
	}
	if query := build("lead_p50_hours"); !strings.Contains(query, "avg(if(completed_count > 0, lead_p50_hours, NULL)) AS y") {
		t.Fatalf("lead_p50_hours is not averaged over the rows with a completed item:\n%s", query)
	}
	if query := build("completed_count"); !strings.Contains(query, "toFloat64(sum(completed_count)) AS y") {
		t.Fatalf("completed_count is not a plain sum:\n%s", query)
	}
	if got := averageExpression("cicd_metrics_daily", "success_rate"); got != "avg(success_rate)" {
		t.Fatalf("a metric with no registered sample count = %q, want a plain avg", got)
	}
}
