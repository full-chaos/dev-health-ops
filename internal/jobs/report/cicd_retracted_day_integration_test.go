//go:build integration

package report

import (
	"reflect"
	"testing"
)

// A cicd_metrics_daily repository-day whose newest row holds retracted = 1 reads as ABSENT in every report reader of the
// table: the stored-row chart of pipelines_count (dedupFromSource) and the key gate of the pooled CI/CD source
// (cicdRunsSource). The rule is applied to the newest row of the key, never before it is chosen: an older live row under
// a newer retraction row is not served, and a live row newer than a retraction row is served.

const (
	retiredRepo = "14016000-0000-4000-8000-0000000000a1"
	retiredDay  = "2026-01-10"
	retiredNext = "2026-01-11"
)

// retireCICDDay writes the row that retires a repository-day: the key, a computed_at one second after the newest stored
// row of the key, retracted = 1 and the column default in every measure.
func (e *pooledEnv) retireCICDDay(org, repo, day string) {
	e.t.Helper()
	e.exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, computed_at, retracted)
SELECT org_id, repo_id, day, max(computed_at) + 1, 1 FROM cicd_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? GROUP BY org_id, repo_id, day`, org, repo, day)
}

// liveCICDDay writes a live row one second after the newest stored row of the key.
func (e *pooledEnv) liveCICDDay(org, repo, day string, pipelines uint32) {
	e.t.Helper()
	e.exec(`INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, computed_at)
SELECT org_id, repo_id, day, ?, 1, max(computed_at) + 1 FROM cicd_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? GROUP BY org_id, repo_id, day`, pipelines, org, repo, day)
}

func mustPoints(t *testing.T, label string, got, want map[string]float64) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", label, got, want)
	}
}

// (a), reader side: a rerun moved the run's finish day from D to D+1; the row that D's compute wrote before the move is
// retired. D reads absent; the window counts the run once.
func TestRegistryCicdRetiredDayReadsAbsent(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "r1", status: "failure", queued: "2026-01-10 23:00:00.000",
		started: "2026-01-10 23:05:00.000", finished: "2026-01-10 23:35:00.000", synced: "2026-01-10 23:40:00.000"})
	e.computeCICD(org, []string{retiredRepo}, retiredDay)
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "r1", status: "success", queued: "2026-01-11 00:10:00.000",
		started: "2026-01-11 00:15:00.000", finished: "2026-01-11 00:25:00.000", synced: "2026-01-11 00:30:00.000"})
	e.computeCICD(org, []string{retiredRepo}, retiredNext)
	e.retireCICDDay(org, retiredRepo, retiredDay)

	window := cicdSpec{start: retiredDay, end: retiredNext}
	byDay := window
	byDay.groupBy = "day"
	mustPoints(t, "pipelines_count by day", pointsOf(e.cicdChart(org, "pipelines_count", byDay)), map[string]float64{retiredNext: 1})
	byRepo := window
	byRepo.groupBy = "repo"
	mustPoints(t, "pipelines_count by repo", pointsOf(e.cicdChart(org, "pipelines_count", byRepo)), map[string]float64{retiredRepo: 1})
	scorecard := window
	scorecard.chartType = "scorecard"
	mustPoints(t, "pipelines_count scorecard", pointsOf(e.cicdChart(org, "pipelines_count", scorecard)), map[string]float64{"total": 1})
	for _, metric := range cicdPooledMetrics {
		want := map[string]float64{"avg_duration_minutes": 10, "p90_duration_minutes": 10, "avg_queue_minutes": 5}[metric]
		mustPoints(t, metric+" by day", pointsOf(e.cicdChart(org, metric, byDay)), map[string]float64{retiredNext: want})
	}
}

// (c): a retired day is never a measurement of 0, and the pooled source does not admit a run of a retired day (a run
// synced late, after the day's newest row retired it) until the day is computed again.
func TestRegistryCicdRetiredDayAdmitsNoLateRun(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "r1", status: "success", queued: "2026-01-10 10:00:00.000",
		started: "2026-01-10 10:05:00.000", finished: "2026-01-10 10:35:00.000", synced: "2026-01-10 10:40:00.000"})
	e.computeCICD(org, []string{retiredRepo}, retiredDay)
	e.retireCICDDay(org, retiredRepo, retiredDay)
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "r1", status: "success", queued: "2026-01-11 00:10:00.000",
		started: "2026-01-11 00:15:00.000", finished: "2026-01-11 00:25:00.000", synced: "2026-01-11 00:30:00.000"})
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "late", status: "success", queued: "2026-01-10 12:00:00.000",
		started: "2026-01-10 12:00:00.000", finished: "2026-01-10 12:40:00.000", synced: "2026-01-11 02:00:00.000"})

	byDay := cicdSpec{groupBy: "day", start: retiredDay, end: retiredNext}
	mustPoints(t, "pipelines_count by day", pointsOf(e.cicdChart(org, "pipelines_count", byDay)), map[string]float64{})
	for _, metric := range cicdPooledMetrics {
		mustPoints(t, metric+" by day", pointsOf(e.cicdChart(org, metric, byDay)), map[string]float64{})
		mustPoints(t, metric+" scorecard", pointsOf(e.cicdChart(org, metric, cicdSpec{chartType: "scorecard", start: retiredDay, end: retiredNext})),
			map[string]float64{})
	}
}

// The newest row decides: a live row newer than a retraction row is served again (with its own values), and the pooled
// source admits the day's runs again.
func TestRegistryCicdLiveRowAfterARetractionIsServed(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	e.insertCICDRuns(org, retiredRepo, cicdRun{id: "r1", status: "success", queued: "2026-01-10 10:00:00.000",
		started: "2026-01-10 10:05:00.000", finished: "2026-01-10 10:35:00.000", synced: "2026-01-10 10:40:00.000"})
	e.computeCICD(org, []string{retiredRepo}, retiredDay)
	e.retireCICDDay(org, retiredRepo, retiredDay)
	e.liveCICDDay(org, retiredRepo, retiredDay, 3)

	byDay := cicdSpec{groupBy: "day", start: retiredDay, end: retiredDay}
	mustPoints(t, "pipelines_count by day", pointsOf(e.cicdChart(org, "pipelines_count", byDay)), map[string]float64{retiredDay: 3})
	mustPoints(t, "avg_duration_minutes by day", pointsOf(e.cicdChart(org, "avg_duration_minutes", byDay)), map[string]float64{retiredDay: 30})
}

// Live rows only (today's data): every grouping of every CI/CD metric reads the values the build before the retracted
// column read. The values are the ones that build gives on this seed.
func TestRegistryCicdLiveRowsReadAsBefore(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const other = "14016000-0000-4000-8000-0000000000b1"
	seedReportCell(e, org, retiredRepo)
	e.insertCICDRuns(org, other, cicdRun{id: "o-x", status: "failure", queued: "2026-01-03 06:55:00.000",
		started: "2026-01-03 07:00:00.000", finished: "2026-01-03 07:30:00.000"})
	e.computeCICD(org, []string{retiredRepo, other}, "2026-01-02", "2026-01-03")

	window := cicdSpec{start: "2026-01-01", end: "2026-01-06"}
	with := func(groupBy, chartType string) cicdSpec {
		spec := window
		spec.groupBy, spec.chartType = groupBy, chartType
		return spec
	}
	want := map[string]map[string]map[string]float64{
		"pipelines_count": {
			"day":       {"2026-01-02": 2, "2026-01-03": 9},
			"repo":      {retiredRepo: 10, other: 1},
			"scorecard": {"total": 11},
		},
		"avg_duration_minutes": {
			"day":       {"2026-01-02": 15, "2026-01-03": 56.666666666666664},
			"repo":      {retiredRepo: 51, other: 30},
			"scorecard": {"total": 49.09090909090909},
		},
		"p90_duration_minutes": {
			"day":       {"2026-01-02": 19, "2026-01-03": 60},
			"repo":      {retiredRepo: 60, other: 30},
			"scorecard": {"total": 60},
		},
		"avg_queue_minutes": {
			"day":       {"2026-01-02": 2, "2026-01-03": 9.444444444444445},
			"repo":      {retiredRepo: 8.4, other: 5},
			"scorecard": {"total": 8.090909090909092},
		},
	}
	for metric, forms := range want {
		for form, points := range forms {
			spec := with(form, "")
			if form == "scorecard" {
				spec = with("", "scorecard")
			}
			got := pointsOf(e.cicdChart(org, metric, spec))
			if len(got) != len(points) {
				t.Errorf("%s %s = %v, want %v", metric, form, got, points)
				continue
			}
			for x, value := range points {
				if !near(got[x], value) {
					t.Errorf("%s %s = %v, want %v", metric, form, got, points)
					break
				}
			}
		}
	}
}
