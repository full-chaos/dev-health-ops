//go:build integration

package report

import (
	"testing"
	"time"
)

// The real cicd writer retires a day whose run a rerun moved (CHAOS-14016) after the day was computed with its pooled
// samples stored (CHAOS-14025). Every CI/CD chart reads the day as absent: the stored samples of the older live row are
// not served, and a run synced late on the retired day does not enter through the raw-run arm.
func TestRegistryCicdWriterRetiredDayIsAbsentInEveryChart(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "14016000-0000-4000-8000-0000000000d7"
	e.insertCICDRuns(org, repo, cicdRun{id: "r1", status: "success", queued: "2026-01-10 10:00:00.000",
		started: "2026-01-10 10:05:00.000", finished: "2026-01-10 10:35:00.000", synced: "2026-01-10 10:40:00.000"})
	e.computeCICD(org, []string{repo}, "2026-01-10")
	byDay := cicdSpec{groupBy: "day", start: "2026-01-10", end: "2026-01-11"}
	mustPoints(t, "avg_duration_minutes before the rerun", pointsOf(e.cicdChart(org, "avg_duration_minutes", byDay)), map[string]float64{"2026-01-10": 30})

	e.insertCICDRuns(org, repo, cicdRun{id: "r1", status: "success", queued: "2026-01-11 00:10:00.000",
		started: "2026-01-11 00:15:00.000", finished: "2026-01-11 00:25:00.000", synced: "2026-01-11 00:30:00.000"})
	time.Sleep(1100 * time.Millisecond) // computed_at is whole seconds: the recompute reads in a later second
	e.computeCICD(org, []string{repo}, "2026-01-10", "2026-01-11")
	e.insertCICDRuns(org, repo, cicdRun{id: "late", status: "success", queued: "2026-01-10 12:00:00.000",
		started: "2026-01-10 12:00:00.000", finished: "2026-01-10 12:40:00.000", synced: "2026-01-11 02:00:00.000"})

	mustPoints(t, "pipelines_count by day", pointsOf(e.cicdChart(org, "pipelines_count", byDay)), map[string]float64{"2026-01-11": 1})
	want := map[string]float64{"avg_duration_minutes": 10, "p90_duration_minutes": 10, "avg_queue_minutes": 5}
	for _, metric := range cicdPooledMetrics {
		mustPoints(t, metric+" by day", pointsOf(e.cicdChart(org, metric, byDay)), map[string]float64{"2026-01-11": want[metric]})
	}
}
