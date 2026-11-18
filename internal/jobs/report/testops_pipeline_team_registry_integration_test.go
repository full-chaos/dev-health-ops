//go:build integration

package report

import (
	"strings"
	"testing"
	"time"
)

// testopsPipelineStoredRow is the writer's newest row of one repository-day of testops_pipeline_metrics_daily.
type testopsPipelineStoredRow struct {
	success, failure, cancelled, cancelRate, rerunRate float64
	team                                               string
}

func (e *pooledEnv) storedPipelineRows(org string) map[string]testopsPipelineStoredRow {
	e.t.Helper()
	rows, err := e.conn.Query(e.ctx, `SELECT toString(repo_id), toFloat64(success_count), toFloat64(failure_count), toFloat64(cancelled_count),
    cancel_rate, rerun_rate, ifNull(team_id, '')
FROM testops_pipeline_metrics_daily WHERE org_id = ? ORDER BY computed_at DESC LIMIT 1 BY repo_id, day`, org)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]testopsPipelineStoredRow{}
	for rows.Next() {
		var repo string
		var row testopsPipelineStoredRow
		if err := rows.Scan(&repo, &row.success, &row.failure, &row.cancelled, &row.cancelRate, &row.rerunRate, &row.team); err != nil {
			e.t.Fatal(err)
		}
		out[repo] = row
	}
	return out
}

// testopsPipelineWant is the value of a bucket of rows, or false for no point: counts are sums; the success and failure
// rates are the summed successes (failures) over the summed completed runs; cancel_rate and rerun_rate are the mean of the
// stored repository-day values (ticket 14108 changes that rule; this cell only moves the rows a team reads).
func testopsPipelineWant(metric string, rows []testopsPipelineStoredRow) (float64, bool) {
	var success, failure, cancelled, cancelRate, rerunRate float64
	for _, row := range rows {
		success, failure, cancelled = success+row.success, failure+row.failure, cancelled+row.cancelled
		cancelRate, rerunRate = cancelRate+row.cancelRate, rerunRate+row.rerunRate
	}
	switch metric {
	case "success_count":
		return success, true
	case "failure_count":
		return failure, true
	case "cancelled_count":
		return cancelled, true
	case "success_rate", "failure_rate":
		if success+failure == 0 {
			return 0, false
		}
		if metric == "success_rate" {
			return success / (success + failure), true
		}
		return failure / (success + failure), true
	case "cancel_rate":
		if len(rows) == 0 {
			return 0, false
		}
		return cancelRate / float64(len(rows)), true
	default:
		if len(rows) == 0 {
			return 0, false
		}
		return rerunRate / float64(len(rows)), true
	}
}

func (e *pooledEnv) pipelineChart(org, metric, groupBy, chartType, start, end string, teams ...string) map[string]float64 {
	e.t.Helper()
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: chartType, Metric: metric, GroupBy: groupBy, OrganizationID: org,
		TimeRangeStart: start, TimeRangeEnd: end, FilterTeams: teams})
	if err != nil {
		e.t.Fatalf("%s chart: %v", metric, err)
	}
	if result.SourceTable != "testops_pipeline_metrics_daily" {
		e.t.Fatalf("%s chart names %s, want testops_pipeline_metrics_daily", metric, result.SourceTable)
	}
	return pointsOf(result)
}

// mustEqualBuckets holds a chart to exactly the buckets of want: each bucket's value, or no point where the rows give none.
func mustEqualBuckets(t *testing.T, what, metric string, got map[string]float64, want map[string][]testopsPipelineStoredRow, chartType string) {
	t.Helper()
	points := map[string]float64{}
	for key, rows := range want {
		if len(rows) == 0 && chartType != "scorecard" {
			continue // a bar groups the rows: no row is no bar, a count too
		}
		if value, ok := testopsPipelineWant(metric, rows); ok {
			points[key] = value
		}
	}
	if len(got) != len(points) {
		t.Errorf("%s %s: got %v, want %v", metric, what, got, points)
		return
	}
	for key, value := range points {
		mustEqual(t, metric+" "+what, got, key, value)
	}
}

// A team of a TestOps pipeline chart is the owner of the repository as of the report run (2026-03-01), never the stored
// team_id (the main owner at the end of the day). Team-1 owns A; team-1 and team-2 co-own B (the writer stores team-1);
// nobody owns C; team-old owned D until 2026-02-01 (the writer stores team-old), team-new owns it since. The runs are
// written to ci_pipeline_runs and the real TestOps daily writers compute 2026-01-02.
func TestRegistryTestopsPipelineTeamIsRepositoryOwnership(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const (
		repoA = "14004000-0000-4000-8000-0000000000a1"
		repoB = "14004000-0000-4000-8000-0000000000b1"
		repoC = "14004000-0000-4000-8000-0000000000c1"
		repoD = "14004000-0000-4000-8000-0000000000d1"
	)
	e.own(org, "team-1", repoA)
	e.own(org, "team-1", repoB)
	e.ownAlso(org, "team-2", repoB, "")
	e.exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/0000d1', 'github', ?, now(), now())`, repoD, org)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-old', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2025-01-01 00:00:00', '2026-02-01 00:00:00', '2025-01-01 00:00:00')`, org, repoD)
	e.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-new', ?, 'acme/0000d1', 'exact', 'inferred', 1, 0, 0, '2026-02-01 00:00:00', NULL, '2026-02-01 00:00:00')`, org, repoD)
	run := func(id, status string, minute int, retry uint32) cicdRun {
		at := time.Date(2026, 1, 2, 10, minute, 0, 0, time.UTC)
		const f = "2006-01-02 15:04:05.000"
		return cicdRun{id: id, status: status, queued: at.Add(-time.Minute).Format(f), started: at.Format(f), finished: at.Add(5 * time.Minute).Format(f), retry: retry}
	}
	e.insertCICDRuns(org, repoA, run("a1", "success", 0, 0), run("a2", "success", 1, 0), run("a3", "success", 2, 1), run("a4", "failure", 3, 0), run("a5", "cancelled", 4, 0))
	e.insertCICDRuns(org, repoB, run("b1", "success", 0, 0), run("b2", "failure", 1, 0), run("b3", "failure", 2, 0), run("b4", "failure", 3, 1))
	e.insertCICDRuns(org, repoC, run("c1", "success", 0, 0), run("c2", "success", 1, 0))
	e.insertCICDRuns(org, repoD, run("d1", "success", 0, 0), run("d2", "failure", 1, 0), run("d3", "cancelled", 2, 0), run("d4", "cancelled", 3, 0))
	e.computeDaily(org, []string{repoA, repoB, repoC, repoD}, "2026-01-02")

	stored := e.storedPipelineRows(org)
	for repo, team := range map[string]string{repoA: "team-1", repoB: "team-1", repoC: "", repoD: "team-old"} {
		if row, ok := stored[repo]; !ok || row.team != team {
			t.Fatalf("the writer stored %v for %s, want team_id %q (the main owner at the end of the day)", row, repo, team)
		}
	}
	of := func(repos ...string) []testopsPipelineStoredRow {
		out := []testopsPipelineStoredRow{}
		for _, repo := range repos {
			out = append(out, stored[repo])
		}
		return out
	}
	const start, end = "2026-01-01", "2026-01-06"
	all := of(repoA, repoB, repoC, repoD)
	for _, metric := range testopsPipelineStoredKeys {
		chart := func(groupBy, chartType string, teams ...string) map[string]float64 {
			return e.pipelineChart(org, metric, groupBy, chartType, start, end, teams...)
		}
		for _, chartType := range []string{"scorecard", "bar"} {
			mustEqualBuckets(t, chartType+" organization", metric, chart("", chartType), map[string][]testopsPipelineStoredRow{"total": all}, chartType)
			mustEqualBuckets(t, chartType+" team-1", metric, chart("", chartType, "team-1"), map[string][]testopsPipelineStoredRow{"total": of(repoA, repoB)}, chartType)
			mustEqualBuckets(t, chartType+" co-owner team-2", metric, chart("", chartType, "team-2"), map[string][]testopsPipelineStoredRow{"total": of(repoB)}, chartType)
			mustEqualBuckets(t, chartType+" team-1 and team-2, B once", metric, chart("", chartType, "team-1", "team-2"), map[string][]testopsPipelineStoredRow{"total": of(repoA, repoB)}, chartType)
			mustEqualBuckets(t, chartType+" owner at the run team-new", metric, chart("", chartType, "team-new"), map[string][]testopsPipelineStoredRow{"total": of(repoD)}, chartType)
			for _, team := range []string{"team-old", "team-none"} {
				mustEqualBuckets(t, chartType+" "+team+" owns no repository at the run", metric, chart("", chartType, team), map[string][]testopsPipelineStoredRow{"total": {}}, chartType)
			}
		}
		mustEqualBuckets(t, "team group", metric, chart("team", "bar"),
			map[string][]testopsPipelineStoredRow{"team-1": of(repoA, repoB), "team-2": of(repoB), "team-new": of(repoD)}, "bar")
		mustEqualBuckets(t, "team group with filter team-2", metric, chart("team", "bar", "team-2"),
			map[string][]testopsPipelineStoredRow{"team-2": of(repoB)}, "bar")
		// The service group is unchanged: the stored service (NULL here, one "" bucket); success_rate has no service dimension.
		service := ""
		if metric == "success_rate" {
			service = "unscoped"
		}
		mustEqualBuckets(t, "service group", metric, chart("service", "bar"), map[string][]testopsPipelineStoredRow{service: all}, "bar")
	}
	rendered := renderChart(func() ChartResult {
		adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
		result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: "bar", Metric: "success_rate", GroupBy: "team", OrganizationID: org, TimeRangeStart: start, TimeRangeEnd: end})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}())
	if !strings.Contains(rendered, "| team-1 |") || !strings.Contains(rendered, "| team-2 |") || !strings.Contains(rendered, "| team-new |") ||
		strings.Contains(rendered, "unscoped") || strings.Contains(rendered, "team-old") || strings.Contains(rendered, "\n|  | ") {
		t.Errorf("rendered team chart:\n%s", rendered)
	}
}

// A mean over no row is no value (D3): an organization scorecard of cancel_rate or rerun_rate over a window with no row
// has no point, never NaN; the counts stay 0 and the success and failure rates have no point, as before. With rows, every
// organization value is the same as before.
func TestRegistryTestopsPipelineEmptyWindowIsNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14004000-0000-4000-8000-0000000000e1"
	e.own(org, "team-1", repo)
	at := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	const f = "2006-01-02 15:04:05.000"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "e1", status: "success", queued: at.Format(f), started: at.Format(f), finished: at.Add(time.Minute).Format(f)},
		cicdRun{id: "e2", status: "cancelled", queued: at.Format(f), started: at.Add(time.Minute).Format(f), finished: at.Add(2 * time.Minute).Format(f), retry: 1},
	)
	e.computeDaily(org, []string{repo}, "2026-01-02")
	stored := e.storedPipelineRows(org)
	withRows := []testopsPipelineStoredRow{stored[repo]}
	for _, metric := range testopsPipelineStoredKeys {
		for _, chartType := range []string{"scorecard", "trend_delta", "table"} {
			mustEqualBuckets(t, chartType+" organization with rows", metric, e.pipelineChart(org, metric, "", chartType, "2026-01-01", "2026-01-06"),
				map[string][]testopsPipelineStoredRow{"total": withRows}, "scorecard")
			mustEqualBuckets(t, chartType+" organization, window with no row", metric, e.pipelineChart(org, metric, "", chartType, "2026-02-01", "2026-02-06"),
				map[string][]testopsPipelineStoredRow{"total": {}}, "scorecard")
			mustEqualBuckets(t, chartType+" organization with no row at all", metric, e.pipelineChart(e.newOrg(), metric, "", chartType, "2026-01-01", "2026-01-06"),
				map[string][]testopsPipelineStoredRow{"total": {}}, "scorecard")
		}
	}
}
