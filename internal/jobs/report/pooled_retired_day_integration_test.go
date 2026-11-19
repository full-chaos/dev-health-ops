//go:build integration

package report

// A retired repository-day (its newest row holds retracted = 1, a row written with the column defaults, so its
// pooled_samples_stored is 0) is absent in BOTH arms of every pooled source: the older row's stored samples are not served,
// and a raw row synced after the retraction does not enter through the raw arm (the retracted flag wins over the pooled
// marker). A live repository-day next to it still reads its samples. One cell per pooled source; the cicd source has its own
// cell (TestPooledCICDRetiredDayIsAbsentInBothArms).

import (
	"fmt"
	"testing"
	"time"
)

// retireDay writes the row that retires a repository-day: the key, a computed_at one second after the newest stored row of the
// key, retracted = 1 and the column default in every other column (the writers' marker row).
func (e *pooledEnv) retireDay(table, org, repo, day string) {
	e.t.Helper()
	e.exec(fmt.Sprintf(`INSERT INTO %s (org_id, repo_id, day, computed_at, retracted)
SELECT org_id, repo_id, day, max(computed_at) + 1, 1 FROM %s
WHERE org_id = ? AND repo_id = ? AND day = ? GROUP BY org_id, repo_id, day`, table, table), org, repo, day)
}

func (e *pooledEnv) newestRetracted(table, org, repo, day string) (retracted, marker uint8) {
	e.t.Helper()
	if err := e.conn.QueryRow(e.ctx, fmt.Sprintf(`SELECT argMax(retracted, computed_at), argMax(pooled_samples_stored, computed_at) FROM %s
WHERE org_id = ? AND repo_id = ? AND day = ?`, table), org, repo, day).Scan(&retracted, &marker); err != nil {
		e.t.Fatal(err)
	}
	return retracted, marker
}

// retiredDayAbsent fails when any of the metrics has a point on the retired day, in a scorecard or a bar chart.
func (e *pooledEnv) retiredDayAbsent(cell, phase, table, org, repo, day string, metrics []string) {
	e.t.Helper()
	if retracted, marker := e.newestRetracted(table, org, repo, day); retracted != 1 || marker != 0 {
		e.t.Fatalf("%s %s: the newest row of %s holds retracted %d, pooled_samples_stored %d; want the retraction row (1, 0)",
			cell, phase, day, retracted, marker)
	}
	for _, metric := range metrics {
		for _, chartType := range []string{"scorecard", "bar"} {
			got := pointsOf(e.chart(org, metric, "", chartType, day, day))
			e.t.Logf("RETIRED14148 %-9s %-6s %-26s %-9s points=%v", cell, phase, metric, chartType, got)
			if len(got) != 0 {
				e.t.Errorf("%s %s %s %s: the retired day has points %v, want none (absent, never 0)", cell, phase, metric, chartType, got)
			}
		}
	}
}

// liveDayServed fails when a live repository-day does not chart its stored value.
func (e *pooledEnv) liveDayServed(cell, table, org, repo, day string, metrics []string) {
	e.t.Helper()
	for _, metric := range metrics {
		stored, storedOK := e.storedDelivery(org, repo, table, metric)[day]
		chart, chartOK := pointsOf(e.chart(org, metric, "", "scorecard", day, day))["total"]
		e.t.Logf("RETIRED14148 %-9s live   %-26s stored=%v(%v) chart=%v(%v)", cell, metric, stored, storedOK, chart, chartOK)
		if !storedOK || !chartOK || !near(stored, chart) {
			e.t.Errorf("%s live %s: stored %v (%v), chart %v (%v); want the live day charted at its stored value", cell, metric, stored, storedOK, chart, chartOK)
		}
	}
}

var retiredPipelineMetrics = []string{"p95_duration_seconds", "median_duration_seconds", "avg_queue_seconds", "p95_queue_seconds"}

func TestPooledTestopsPipelineRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14148000-0000-4000-8000-0000000001a1"
	const day, live = "2026-01-02", "2026-01-03"
	const table = "testops_pipeline_metrics_daily"
	e.insertRepo(org, repo)
	e.insertCICDRuns(org, repo,
		cicdRun{id: "rp-1", status: "success", queued: "2026-01-02 00:29:00.000", started: "2026-01-02 00:30:00.000", finished: "2026-01-02 00:40:00.000"},
		cicdRun{id: "rp-2", status: "success", queued: "2026-01-03 00:57:00.000", started: "2026-01-03 01:00:00.000", finished: "2026-01-03 01:20:00.000"},
	)
	e.computeDaily(org, []string{repo}, day, live)
	writerTick()
	e.retireDay(table, org, repo, day)
	e.retiredDayAbsent("pipeline", "stored", table, org, repo, day, retiredPipelineMetrics)
	e.insertCICDRuns(org, repo,
		cicdRun{id: "rp-late", status: "success", queued: "2026-01-02 04:50:00.000", started: "2026-01-02 05:00:00.000", finished: "2026-01-02 06:00:00.000"})
	e.retiredDayAbsent("pipeline", "raw", table, org, repo, day, retiredPipelineMetrics)
	e.liveDayServed("pipeline", table, org, repo, live, retiredPipelineMetrics)
}

func TestPooledDeployRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14148000-0000-4000-8000-0000000001b1"
	const table = "deploy_metrics_daily"
	day, live := deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly)
	metrics := []string{"deploy_time_p50_hours"}
	e.insertRepo(org, repo)
	e.insertDeployment(org, repo, "rd-1", "success", deliveryDay1.Add(time.Hour), 2)
	e.insertDeployment(org, repo, "rd-2", "success", deliveryDay2.Add(time.Hour), 3)
	e.computeDelivery(org, []string{repo}, day, live)
	writerTick()
	e.retireDay(table, org, repo, day)
	e.retiredDayAbsent("deploy", "stored", table, org, repo, day, metrics)
	e.insertDeployment(org, repo, "rd-late", "success", deliveryDay1.Add(5*time.Hour), 7)
	e.retiredDayAbsent("deploy", "raw", table, org, repo, day, metrics)
	e.liveDayServed("deploy", table, org, repo, live, metrics)
}

func TestPooledPullRequestSizeRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14148000-0000-4000-8000-0000000001c1"
	const table = "repo_metrics_daily"
	day, live := deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly)
	metrics := []string{"pr_size_p50_loc", "pr_size_p90_loc"}
	e.insertRepo(org, repo)
	e.insertPullRequest(org, repo, 1, deliveryDay1.Add(time.Hour), 10, 10)
	e.insertPullRequest(org, repo, 2, deliveryDay2.Add(time.Hour), 30, 30)
	e.computeDelivery(org, []string{repo}, day, live)
	writerTick()
	e.retireDay(table, org, repo, day)
	e.retiredDayAbsent("prsize", "stored", table, org, repo, day, metrics)
	e.insertPullRequest(org, repo, 3, deliveryDay1.Add(5*time.Hour), 100, 100)
	e.retiredDayAbsent("prsize", "raw", table, org, repo, day, metrics)
	e.liveDayServed("prsize", table, org, repo, live, metrics)
}

func TestPooledBugRepairRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14148000-0000-4000-8000-0000000001d1"
	const table = "repo_metrics_daily"
	day, live := deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly)
	metrics := []string{"mttr_hours"}
	e.insertRepo(org, repo)
	// A repo_metrics_daily row needs a commit or a pull request on the day; the bug gives the samples.
	e.insertPullRequest(org, repo, 1, deliveryDay1.Add(time.Hour), 10, 10)
	e.insertPullRequest(org, repo, 2, deliveryDay2.Add(time.Hour), 10, 10)
	e.insertBug(org, repo, "rb-1", "bug", deliveryDay1.Add(time.Hour), deliveryDay1.Add(3*time.Hour))
	e.insertBug(org, repo, "rb-2", "bug", deliveryDay2.Add(time.Hour), deliveryDay2.Add(5*time.Hour))
	e.computeDelivery(org, []string{repo}, day, live)
	writerTick()
	e.retireDay(table, org, repo, day)
	e.retiredDayAbsent("bugrepair", "stored", table, org, repo, day, metrics)
	e.insertBug(org, repo, "rb-late", "bug", deliveryDay1.Add(5*time.Hour), deliveryDay1.Add(17*time.Hour))
	e.retiredDayAbsent("bugrepair", "raw", table, org, repo, day, metrics)
	e.liveDayServed("bugrepair", table, org, repo, live, metrics)
}

func TestPooledIncidentRetiredDayIsAbsentInBothArms(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo, service = "14148000-0000-4000-8000-0000000001e1", "svc-retired"
	const table = "incident_metrics_daily"
	day, live := deliveryDay1.Format(time.DateOnly), deliveryDay2.Format(time.DateOnly)
	metrics := []string{"mttr_p50_hours", "mttr_p90_hours"}
	e.insertRepo(org, repo)
	e.mapIncidentService(org, service, repo)
	e.insertIncident(org, service, "ri-1", deliveryDay1.Add(time.Hour), deliveryDay1.Add(3*time.Hour), 0)
	e.insertIncident(org, service, "ri-2", deliveryDay2.Add(time.Hour), deliveryDay2.Add(5*time.Hour), 0)
	e.computeDelivery(org, []string{repo}, day, live)
	writerTick()
	e.retireDay(table, org, repo, day)
	e.retiredDayAbsent("incident", "stored", table, org, repo, day, metrics)
	e.insertIncident(org, service, "ri-late", deliveryDay1.Add(5*time.Hour), deliveryDay1.Add(17*time.Hour), 0)
	e.retiredDayAbsent("incident", "raw", table, org, repo, day, metrics)
	e.liveDayServed("incident", table, org, repo, live, metrics)
}
