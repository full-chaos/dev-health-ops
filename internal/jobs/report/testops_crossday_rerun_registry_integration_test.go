//go:build integration

package report

import (
	"testing"
	"time"
)

// CHAOS-13926 (F3): the report's quality group reads the STORED pass_rate (render.go "quality", metric_registry.json). A run
// re-run on the next day must leave no point on its first day: no stored 0 rate. The run syncs through the production route and
// sink and the day rows come from the production daily writers.
func TestRegistryTestopsCrossDayRerunLeavesNoPassRateOnTheFirstDay(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repo := e.collectAttempt(org, &attemptDoer{t: t, runID: 9301, attempt: 1, started: "2026-01-02T10:00:00Z",
		artifacts: []string{attemptArtifact(77, "2026-01-02T10:05:00Z")}, zips: map[string][]string{"77": {"10"}}},
		time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC))
	e.collectAttempt(org, &attemptDoer{t: t, runID: 9301, attempt: 2, started: "2026-01-03T00:10:00Z",
		artifacts: []string{attemptArtifact(77, "2026-01-02T10:05:00Z"), attemptArtifact(78, "2026-01-03T00:15:00Z")},
		zips:      map[string][]string{"77": {"10"}, "78": {"12"}}},
		time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	e.own(org, "team-r", repo)
	e.computeDaily(org, []string{repo}, "2026-01-02", "2026-01-03")

	points := map[string]float64{}
	for _, point := range e.chart(org, "pass_rate", "", "line", "2026-01-01", "2026-01-06").DataPoints {
		points[point.X] = point.Y
	}
	t.Logf("F3CELL REPORT pass_rate points=%v", points)
	if value, ok := points["2026-01-02"]; ok {
		t.Errorf("the re-run's first day 2026-01-02 has a pass_rate point %v; want none (no stored rate for a day with no counted case)", value)
	}
	if value, ok := points["2026-01-03"]; !ok || !near(value, 1) {
		t.Errorf("the newest attempt's day 2026-01-03: point %v (present %t); want 1", value, ok)
	}
}
