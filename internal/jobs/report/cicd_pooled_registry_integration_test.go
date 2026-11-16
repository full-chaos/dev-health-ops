//go:build integration

package report

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// The AI report charts the CI/CD duration and queue metrics over the window's runs, with the cicd daily writer's own run
// and sample rules, never as the mean of the stored per-repository-day values. Every cell inserts raw ci_pipeline_runs
// rows (the edge rows below, such as a stored duration_seconds that disagrees with the timestamps, are not producible by
// the provider routes), runs the REAL cicd daily writer (daily.NewCICDExecutor) and charts through the real adapter.

var cicdPooledMetrics = []string{"avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes"}

// cicdRun is one raw run. An empty queued or finished time is NULL; status nil is NULL.
type cicdRun struct {
	id, queued, started, finished string
	status                        any
	synced                        string
	retry                         uint32
	durationS, queueS             any
}

func (e *pooledEnv) insertCICDRuns(org, repo string, runs ...cicdRun) {
	e.t.Helper()
	nullable := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	for _, run := range runs {
		synced := run.synced
		if synced == "" {
			synced = "2026-02-01 00:00:00.000"
		}
		// One INSERT per row: two versions of one run stay two parts, so FINAL is load-bearing.
		e.exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, queued_at, started_at, finished_at, last_synced, org_id, retry_count, duration_seconds, queue_seconds)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repo, run.id, run.status, nullable(run.queued), run.started, nullable(run.finished), synced, org, run.retry, run.durationS, run.queueS)
	}
}

func (e *pooledEnv) computeCICD(org string, repos []string, days ...string) {
	e.t.Helper()
	executor, err := daily.NewCICDExecutor(e.conn)
	if err != nil {
		e.t.Fatal(err)
	}
	ids := make([]daily.RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, daily.RepositoryID(repo))
	}
	for i, day := range days {
		target, _ := time.Parse(time.DateOnly, day)
		run := daily.Run{ID: fmt.Sprintf("cicd-%s-%d", org, i), OrganizationID: org, TargetDay: target}
		part := daily.Partition{ID: fmt.Sprintf("cicd-part-%s-%d", org, i), RunID: run.ID, RepoIDs: ids}
		if _, err := executor.ComputeFamily(e.ctx, run, part); err != nil {
			e.t.Fatal(err)
		}
	}
}

type cicdStored struct{ avg, p90, queue *float64 }

// stored reads the writer's newest row of each day of the repository.
func (e *pooledEnv) stored(org, repo string) map[string]cicdStored {
	e.t.Helper()
	rows, err := e.conn.Query(e.ctx, `SELECT day, avg_duration_minutes, p90_duration_minutes, avg_queue_minutes FROM cicd_metrics_daily
WHERE org_id = ? AND repo_id = ? ORDER BY computed_at DESC LIMIT 1 BY day`, org, repo)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]cicdStored{}
	for rows.Next() {
		var day time.Time
		var row cicdStored
		if err := rows.Scan(&day, &row.avg, &row.p90, &row.queue); err != nil {
			e.t.Fatal(err)
		}
		out[day.Format(time.DateOnly)] = row
	}
	return out
}

func (s cicdStored) of(metric string) *float64 {
	switch metric {
	case "avg_duration_minutes":
		return s.avg
	case "p90_duration_minutes":
		return s.p90
	}
	return s.queue
}

type cicdSpec struct {
	groupBy, chartType, start, end string
	teams, repos                   []string
}

func (e *pooledEnv) cicdChart(org, metric string, spec cicdSpec) ChartResult {
	e.t.Helper()
	if spec.chartType == "" {
		spec.chartType = "bar"
	}
	adapter := &ClickHouseQueryAdapter{conn: e.conn, now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.executeChart(e.ctx, ChartSpec{ChartType: spec.chartType, Metric: metric, GroupBy: spec.groupBy, OrganizationID: org,
		TimeRangeStart: spec.start, TimeRangeEnd: spec.end, FilterTeams: spec.teams, FilterRepos: spec.repos})
	if err != nil {
		e.t.Fatalf("%s chart: %v", metric, err)
	}
	if result.SourceTable != "cicd_metrics_daily" {
		e.t.Fatalf("%s chart names %s, want the registry table cicd_metrics_daily", metric, result.SourceTable)
	}
	return result
}

func minutes(values ...float64) []float64 { return values }

// cicdWant is the writer's value of one pool of samples: mean and p90 (linear between closest ranks) of the durations,
// mean of the queue samples.
func cicdWant(durations, queues []float64) map[string]float64 {
	return map[string]float64{
		"avg_duration_minutes": mean(durations),
		"p90_duration_minutes": inclusive(durations, 0.9),
		"avg_queue_minutes":    mean(queues),
	}
}

// The executed numbers of the original report: day 1 two runs of 10 and 20 min queued 1 and 3 min, day 2 eight runs of
// 60 min queued 10 min. The mean of the stored day values gave 37.5 / 39.5 / 6; the window's runs give 51 / 60 / 8.4.
func seedReportCell(e *pooledEnv, org, repo string) {
	e.t.Helper()
	runs := []cicdRun{
		{id: "o-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"},
		{id: "o-2", status: "success", queued: "2026-01-02 00:57:00.000", started: "2026-01-02 01:00:00.000", finished: "2026-01-02 01:20:00.000"},
	}
	for i := 0; i < 8; i++ {
		start := time.Date(2026, 1, 3, 0, 30*(i+1), 0, 0, time.UTC)
		f := "2006-01-02 15:04:05.000"
		runs = append(runs, cicdRun{id: fmt.Sprintf("o-d2-%d", i), status: "success", queued: start.Add(-10 * time.Minute).Format(f),
			started: start.Format(f), finished: start.Add(60 * time.Minute).Format(f)})
	}
	e.insertCICDRuns(org, repo, runs...)
}

func TestRegistryCicdChartsPoolTheWindowRuns(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000a1"
	seedReportCell(e, org, repo)
	e.computeCICD(org, []string{repo}, "2026-01-02", "2026-01-03")
	stored := e.stored(org, repo)
	if day := stored["2026-01-02"]; day.avg == nil || !near(*day.avg, 15) || !near(*day.p90, 19) || !near(*day.queue, 2) {
		t.Fatalf("the writer stored %v for 2026-01-02, want 15 / 19 / 2", stored)
	}
	want := map[string]float64{"avg_duration_minutes": 51, "p90_duration_minutes": 60, "avg_queue_minutes": 8.4}
	for _, metric := range cicdPooledMetrics {
		window := cicdSpec{start: "2026-01-01", end: "2026-01-06"}
		mustEqual(t, metric+" bar", pointsOf(e.cicdChart(org, metric, window)), "total", want[metric])
		window.chartType = "scorecard"
		mustEqual(t, metric+" scorecard", pointsOf(e.cicdChart(org, metric, window)), "total", want[metric])
		week := pointsOf(e.cicdChart(org, metric, cicdSpec{groupBy: "week", start: "2026-01-01", end: "2026-01-06"}))
		mustEqual(t, metric+" week", week, "2025-12-28", want[metric])
		// A one-repository-day bucket is the stored value of that day: 19 is linear between 10 and 20.
		days := pointsOf(e.cicdChart(org, metric, cicdSpec{groupBy: "day", start: "2026-01-01", end: "2026-01-06"}))
		for day, row := range stored {
			mustEqual(t, metric+" day", days, day, *row.of(metric))
		}
		if len(days) != 2 {
			t.Errorf("%s by day: %v, want the two stored days", metric, days)
		}
	}
}

// The CI/CD metrics have no team dimension (the registry lists repo and day): as before, a team filter does not narrow
// the chart and a team group is one 'unscoped' bucket. The rendered report never labels that bucket as a team.
func TestRegistryCicdTeamIsNotADimension(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repoA, repoB = "13885000-0000-4000-8000-0000000000b1", "13885000-0000-4000-8000-0000000000b2"
	e.own(org, "team-1", repoA)
	e.own(org, "team-2", repoB)
	seedReportCell(e, org, repoA)
	e.insertCICDRuns(org, repoB,
		cicdRun{id: "b-1", status: "success", queued: "2026-01-02 02:40:00.000", started: "2026-01-02 03:00:00.000", finished: "2026-01-02 04:40:00.000"},
		cicdRun{id: "b-2", status: "failure", queued: "2026-01-02 04:40:00.000", started: "2026-01-02 05:00:00.000", finished: "2026-01-02 06:40:00.000"},
	)
	e.computeCICD(org, []string{repoA, repoB}, "2026-01-02", "2026-01-03")
	runsA := minutes(10, 20, 60, 60, 60, 60, 60, 60, 60, 60)
	queuesA := minutes(1, 3, 10, 10, 10, 10, 10, 10, 10, 10)
	org12 := cicdWant(append(append([]float64(nil), runsA...), 100, 100), append(append([]float64(nil), queuesA...), 20, 20))
	perRepo := map[string]map[string]float64{repoA: cicdWant(runsA, queuesA), repoB: cicdWant(minutes(100, 100), minutes(20, 20))}
	window := cicdSpec{start: "2026-01-01", end: "2026-01-06"}
	for _, metric := range cicdPooledMetrics {
		mustEqual(t, metric+" org", pointsOf(e.cicdChart(org, metric, window)), "total", org12[metric])
		filtered := window
		filtered.teams = []string{"team-1"}
		mustEqual(t, metric+" team filter (not a dimension)", pointsOf(e.cicdChart(org, metric, filtered)), "total", org12[metric])
		grouped := window
		grouped.groupBy = "team"
		teams := pointsOf(e.cicdChart(org, metric, grouped))
		if len(teams) != 1 {
			t.Errorf("%s by team: %v, want one 'unscoped' bucket", metric, teams)
		}
		mustEqual(t, metric+" team group (not a dimension)", teams, "unscoped", org12[metric])
		grouped.teams = []string{"team-2"}
		mustEqual(t, metric+" team group and filter", pointsOf(e.cicdChart(org, metric, grouped)), "unscoped", org12[metric])
		service := window
		service.groupBy = "service"
		mustEqual(t, metric+" service group (not a dimension)", pointsOf(e.cicdChart(org, metric, service)), "unscoped", org12[metric])
		byRepo := window
		byRepo.groupBy = "repo"
		repos := pointsOf(e.cicdChart(org, metric, byRepo))
		for repo, values := range perRepo {
			mustEqual(t, metric+" by repo", repos, repo, values[metric])
		}
		onlyB := window
		onlyB.repos = []string{repoB}
		mustEqual(t, metric+" repo filter", pointsOf(e.cicdChart(org, metric, onlyB)), "total", perRepo[repoB][metric])
	}
	// The render path: the table prints the bucket under the generic x column, the default title is the registry name and
	// the narrative names no bucket.
	chart := e.cicdChart(org, "avg_duration_minutes", cicdSpec{groupBy: "team", start: "2026-01-01", end: "2026-01-06"})
	rendered := renderChart(chart)
	if !strings.Contains(rendered, "#### Avg Duration Minutes\n") || !strings.Contains(rendered, "| x | y | group |") || !strings.Contains(rendered, "| unscoped |") {
		t.Errorf("rendered chart:\n%s", rendered)
	}
	if strings.Contains(strings.ToLower(rendered), "team") {
		t.Errorf("the rendered chart presents the unscoped bucket as a team:\n%s", rendered)
	}
	if sentence := chartSentence(chart); strings.Contains(sentence, "unscoped") || strings.Contains(strings.ToLower(sentence), "team") {
		t.Errorf("the narrative names the bucket: %s", sentence)
	}
}

// A re-run keeps its run id; the newest synced version replaces the earlier attempt, in the writer and in the chart. The
// queue sample of a re-run spans from the run's first creation (queued_at) to the latest attempt's start.
func TestRegistryCicdRerunCountsTheNewestAttempt(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000c1"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "r-1", status: "failure", queued: "2026-01-04 08:59:00.000", started: "2026-01-04 09:00:00.000", finished: "2026-01-04 09:10:00.000", synced: "2026-01-09 00:00:00.000"},
		cicdRun{id: "r-1", status: "success", queued: "2026-01-04 08:59:00.000", started: "2026-01-04 11:00:00.000", finished: "2026-01-04 11:30:00.000", synced: "2026-01-10 00:00:00.000", retry: 1},
		cicdRun{id: "r-2", status: "success", queued: "2026-01-04 11:59:00.000", started: "2026-01-04 12:00:00.000", finished: "2026-01-04 12:20:00.000"},
		// The newer attempt of r-3 started the next day: the older one (50 min on 01-05) is no sample of 01-05.
		cicdRun{id: "r-3", status: "failure", queued: "2026-01-05 09:59:00.000", started: "2026-01-05 10:00:00.000", finished: "2026-01-05 10:50:00.000", synced: "2026-01-09 00:00:00.000"},
		cicdRun{id: "r-3", status: "success", queued: "2026-01-05 09:59:00.000", started: "2026-01-06 10:00:00.000", finished: "2026-01-06 10:40:00.000", synced: "2026-01-10 00:00:00.000", retry: 1},
		cicdRun{id: "r-4", status: "success", queued: "2026-01-05 11:59:00.000", started: "2026-01-05 12:00:00.000", finished: "2026-01-05 12:10:00.000"},
	)
	var versions uint64
	if err := e.conn.QueryRow(e.ctx, "SELECT count() FROM ci_pipeline_runs WHERE org_id = ? AND run_id IN ('r-1', 'r-3')", org).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 4 {
		t.Fatalf("stored %d unmerged versions of r-1 and r-3, want 4", versions)
	}
	e.computeCICD(org, []string{repo}, "2026-01-04", "2026-01-05", "2026-01-06")
	stored := e.stored(org, repo)
	want := map[string]map[string]float64{
		"2026-01-04": cicdWant(minutes(30, 20), minutes(121, 1)),
		"2026-01-05": cicdWant(minutes(10), minutes(1)),
		"2026-01-06": cicdWant(minutes(40), minutes(24*60+1)),
	}
	for _, metric := range cicdPooledMetrics {
		days := pointsOf(e.cicdChart(org, metric, cicdSpec{groupBy: "day", start: "2026-01-04", end: "2026-01-06"}))
		for day, values := range want {
			mustEqual(t, metric+" writer", map[string]float64{day: *stored[day].of(metric)}, day, values[metric])
			mustEqual(t, metric+" chart", days, day, values[metric])
		}
		window := cicdWant(minutes(30, 20, 10, 40), minutes(121, 1, 1, 24*60+1))
		mustEqual(t, metric+" window", pointsOf(e.cicdChart(org, metric, cicdSpec{start: "2026-01-04", end: "2026-01-06"})), "total", window[metric])
	}
}

// A run counts on day D only when it starts and finishes on D (the writer's double window): a run across midnight counts
// on no day, a run still running counts nowhere, and the chart range includes its first and its last day.
func TestRegistryCicdWindowEdges(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000d1"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "e-first", status: "success", queued: "2026-01-01 23:59:00.000", started: "2026-01-02 00:00:00.000", finished: "2026-01-02 00:10:00.000"},
		cicdRun{id: "e-last", status: "success", queued: "2026-01-03 22:58:00.000", started: "2026-01-03 23:00:00.000", finished: "2026-01-03 23:59:59.999"},
		cicdRun{id: "e-cross-in", status: "success", queued: "2026-01-01 23:49:00.000", started: "2026-01-01 23:50:00.000", finished: "2026-01-02 00:10:00.000"},
		cicdRun{id: "e-cross-out", status: "success", queued: "2026-01-03 23:49:00.000", started: "2026-01-03 23:50:00.000", finished: "2026-01-04 00:10:00.000"},
		cicdRun{id: "e-running", status: "running", queued: "2026-01-03 09:59:00.000", started: "2026-01-03 10:00:00.000"},
		cicdRun{id: "e-before", status: "success", queued: "2026-01-01 11:59:00.000", started: "2026-01-01 12:00:00.000", finished: "2026-01-01 12:20:00.000"},
		cicdRun{id: "e-after", status: "success", queued: "2026-01-03 23:59:00.000", started: "2026-01-04 00:00:00.000", finished: "2026-01-04 00:30:00.000"},
	)
	e.computeCICD(org, []string{repo}, "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04")
	last := 3599999.0 / 60000
	want := cicdWant(minutes(10, last), minutes(1, 2))
	stored := e.stored(org, repo)
	for _, metric := range cicdPooledMetrics {
		mustEqual(t, metric+" window", pointsOf(e.cicdChart(org, metric, cicdSpec{start: "2026-01-02", end: "2026-01-03"})), "total", want[metric])
		mustEqual(t, metric+" scorecard", pointsOf(e.cicdChart(org, metric, cicdSpec{chartType: "scorecard", start: "2026-01-02", end: "2026-01-03"})), "total", want[metric])
		days := pointsOf(e.cicdChart(org, metric, cicdSpec{groupBy: "day", start: "2026-01-01", end: "2026-01-04"}))
		if len(days) != len(stored) {
			t.Errorf("%s by day: %v, want the %d stored days", metric, days, len(stored))
		}
		for day, row := range stored {
			mustEqual(t, metric+" day", days, day, *row.of(metric))
		}
	}
	if day := stored["2026-01-02"]; day.avg == nil || !near(*day.avg, 10) {
		t.Errorf("the writer stored %v for 2026-01-02, want only e-first (10 min)", stored)
	}
}

// On one repository-day the chart is the writer's own value: the samples are the timestamps (never the stored
// duration_seconds or queue_seconds), a negative duration or queue is no sample, a run with no queued_at has no queue
// sample, every status counts except deleted.
func TestRegistryCicdEqualsTheWriterOnOneRepoDay(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000e1"
	wrong := 9999.0
	e.insertCICDRuns(org, repo,
		cicdRun{id: "p-1", status: "success", queued: "2026-01-05 09:50:00.000", started: "2026-01-05 10:00:00.000", finished: "2026-01-05 10:30:00.000", durationS: wrong, queueS: wrong},
		cicdRun{id: "p-2", status: "failure", started: "2026-01-05 11:00:00.000", finished: "2026-01-05 11:05:00.000", durationS: wrong, queueS: wrong},
		cicdRun{id: "p-3", status: "cancelled", queued: "2026-01-05 12:10:00.000", started: "2026-01-05 12:00:00.000", finished: "2026-01-05 12:45:00.000", durationS: wrong},
		cicdRun{id: "p-4", status: nil, queued: "2026-01-05 12:58:00.000", started: "2026-01-05 13:00:00.000", finished: "2026-01-05 12:59:00.000", durationS: wrong},
		cicdRun{id: "p-5", status: "deleted", queued: "2026-01-05 13:00:00.000", started: "2026-01-05 14:00:00.000", finished: "2026-01-05 15:00:00.000"},
		cicdRun{id: "p-6", status: " Deleted ", queued: "2026-01-05 13:00:00.000", started: "2026-01-05 14:00:00.000", finished: "2026-01-05 15:00:00.000"},
		cicdRun{id: "p-7", status: "DELETED", queued: "2026-01-05 13:00:00.000", started: "2026-01-05 14:00:00.000", finished: "2026-01-05 15:00:00.000"},
		cicdRun{id: "p-8", status: "success", queued: "2026-01-05 15:59:30.000", started: "2026-01-05 16:00:00.000", finished: "2026-01-05 16:07:30.500", queueS: wrong},
	)
	e.computeCICD(org, []string{repo}, "2026-01-05")
	stored := e.stored(org, repo)["2026-01-05"]
	want := cicdWant(minutes(30, 5, 45, 450500.0/60000), minutes(10, 2, 0.5))
	for _, metric := range cicdPooledMetrics {
		if value := stored.of(metric); value == nil || !near(*value, want[metric]) {
			t.Fatalf("%s: the writer stored %v, want %v", metric, value, want[metric])
		}
		window := cicdSpec{groupBy: "day", start: "2026-01-05", end: "2026-01-05", repos: []string{repo}}
		mustEqual(t, metric+" day", pointsOf(e.cicdChart(org, metric, window)), "2026-01-05", *stored.of(metric))
		window = cicdSpec{chartType: "scorecard", start: "2026-01-05", end: "2026-01-05"}
		mustEqual(t, metric+" scorecard", pointsOf(e.cicdChart(org, metric, window)), "total", *stored.of(metric))
	}
}

// Only the repository-days the writer computed are read, as the stored charts read them.
func TestRegistryCicdOnlyComputedRepositoryDays(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000f1"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "u-1", status: "success", queued: "2026-01-02 09:59:00.000", started: "2026-01-02 10:00:00.000", finished: "2026-01-02 10:10:00.000"},
		cicdRun{id: "u-2", status: "success", queued: "2026-01-03 09:50:00.000", started: "2026-01-03 10:00:00.000", finished: "2026-01-03 11:00:00.000"},
	)
	e.computeCICD(org, []string{repo}, "2026-01-02")
	for _, metric := range cicdPooledMetrics {
		mustEqual(t, metric, pointsOf(e.cicdChart(org, metric, cicdSpec{start: "2026-01-01", end: "2026-01-06"})), "total", cicdWant(minutes(10), minutes(1))[metric])
	}
}

// A bucket with no sample has no point, never 0.
func TestRegistryCicdNoSampleIsNoPoint(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13885000-0000-4000-8000-0000000000f2"
	e.insertCICDRuns(org, repo,
		cicdRun{id: "n-1", status: "success", started: "2026-01-02 10:00:00.000", finished: "2026-01-02 10:10:00.000"},
	)
	e.computeCICD(org, []string{repo}, "2026-01-02")
	mustEqual(t, "duration with no queue sample", pointsOf(e.cicdChart(org, "avg_duration_minutes", cicdSpec{start: "2026-01-01", end: "2026-01-06"})), "total", 10)
	mustHaveNoPoint(t, "queue with no queue sample", pointsOf(e.cicdChart(org, "avg_queue_minutes", cicdSpec{start: "2026-01-01", end: "2026-01-06"})))
	mustHaveNoPoint(t, "queue scorecard with no queue sample", pointsOf(e.cicdChart(org, "avg_queue_minutes", cicdSpec{chartType: "scorecard", start: "2026-01-01", end: "2026-01-06"})))
	for _, metric := range cicdPooledMetrics {
		mustHaveNoPoint(t, metric+" in a window with no run", pointsOf(e.cicdChart(org, metric, cicdSpec{start: "2026-01-10", end: "2026-01-12"})))
		mustHaveNoPoint(t, metric+" scorecard in a window with no run", pointsOf(e.cicdChart(org, metric, cicdSpec{chartType: "scorecard", start: "2026-01-10", end: "2026-01-12"})))
		mustHaveNoPoint(t, metric+" by team in a window with no run", pointsOf(e.cicdChart(org, metric, cicdSpec{groupBy: "team", start: "2026-01-10", end: "2026-01-12"})))
	}
}
