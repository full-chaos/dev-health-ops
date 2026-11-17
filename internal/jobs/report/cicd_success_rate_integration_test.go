//go:build integration

package report

import (
	"fmt"
	"testing"
	"time"
)

// The AI report charts the window's CI/CD success rate: the successful runs over the runs of the window, never the mean
// of the stored per-repository-day rates (a day of 2 runs would weigh as much as a day of 8). Day 1 has 1 of 2 runs
// succeed (0.5), day 2 has 8 of 8 (1.0): the mean of the day values is 0.75, the window's rate is 9 of 10 = 0.9. Each run
// starts and finishes on its own UTC day.
func TestRegistryCicdSuccessRateIsTheWindowsRate(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	const repo = "13912000-0000-4000-8000-0000000000a1"
	const f = "2006-01-02 15:04:05.000"
	runs := []cicdRun{
		{id: "s-1", status: "success", started: "2026-01-02 10:00:00.000", finished: "2026-01-02 10:10:00.000"},
		{id: "s-2", status: "failure", started: "2026-01-02 11:00:00.000", finished: "2026-01-02 11:10:00.000"},
	}
	for i := 0; i < 8; i++ {
		start := time.Date(2026, 1, 3, 10+i, 0, 0, 0, time.UTC)
		runs = append(runs, cicdRun{id: fmt.Sprintf("s-d2-%d", i), status: "success", started: start.Format(f), finished: start.Add(10 * time.Minute).Format(f)})
	}
	e.insertCICDRuns(org, repo, runs...)
	e.computeCICD(org, []string{repo}, "2026-01-02", "2026-01-03")

	window := cicdSpec{start: "2026-01-01", end: "2026-01-06"}
	mustEqual(t, "success_rate bar", pointsOf(e.cicdChart(org, "success_rate", window)), "total", 0.9)
	window.chartType = "scorecard"
	mustEqual(t, "success_rate scorecard", pointsOf(e.cicdChart(org, "success_rate", window)), "total", 0.9)
	week := pointsOf(e.cicdChart(org, "success_rate", cicdSpec{groupBy: "week", start: "2026-01-01", end: "2026-01-06"}))
	mustEqual(t, "success_rate week", week, "2025-12-28", 0.9)
	days := pointsOf(e.cicdChart(org, "success_rate", cicdSpec{groupBy: "day", start: "2026-01-01", end: "2026-01-06"}))
	mustEqual(t, "success_rate day 1", days, "2026-01-02", 0.5)
	mustEqual(t, "success_rate day 2", days, "2026-01-03", 1.0)
}
