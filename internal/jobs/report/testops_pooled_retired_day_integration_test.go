//go:build integration

package report

import (
	"testing"
)

// CHAOS-14595: the AI report pair (suite_duration_p95_seconds, suite_duration_p50_seconds) over a window with a legacy day
// of repository A (newest row without stored samples, not retracted: read from the raw rows) and a retired day of
// repository B (newest row a stale-key retraction: retracted 1, pooled_samples_stored at its default 0). The served values
// are the pool of the live days only, by total and by team, whether the retired day is there or not.
func TestRegistryTestopsSuitePairWithARetiredDayServesTheLiveDaysOnly(t *testing.T) {
	e := startPooledEnv(t)
	org := e.newOrg()
	repoA := e.seedViaRoute(org, "acme/a", 14595, []seedRun{
		{id: 4001, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 1, suiteTimes: []string{"10", "20", "30"}},
	})
	repoB := e.seedViaRoute(org, "acme/b", 14596, []seedRun{
		{id: 4101, day: "2026-01-02", startMin: 0, durationS: 10, queueS: 1, suiteTimes: []string{"40", "50"}},
		{id: 4102, day: "2026-01-03", startMin: 0, durationS: 10, queueS: 1, suiteTimes: []string{"600"}},
	})
	e.own(org, "team-r", repoA)
	e.own(org, "team-r", repoB)
	e.computeDaily(org, []string{repoA, repoB}, "2026-01-02", "2026-01-03")
	e.exec(`INSERT INTO testops_test_metrics_daily (repo_id, day, org_id, computed_at) VALUES (?, '2026-01-02', ?, now() + INTERVAL 1 HOUR)`, repoA, org)
	live := floats(10, 20, 30, 40, 50)

	check := func(phase string) {
		t.Helper()
		for metric, p := range map[string]float64{"suite_duration_p95_seconds": 0.95, "suite_duration_p50_seconds": 0.5} {
			total := e.value(org, metric, "", "2026-01-01", "2026-01-06")
			byTeam := e.value(org, metric, "team", "2026-01-01", "2026-01-06")
			t.Logf("CELL14595 %s %s: total %v, by team %v, want %v", phase, metric, total, byTeam, inclusive(live, p))
			mustEqual(t, phase+" "+metric+" total", total, "total", inclusive(live, p))
			mustEqual(t, phase+" "+metric+" team-r", byTeam, "team-r", inclusive(live, p))
		}
	}
	// Before B's day is retired, its day2 suite (600) is live: the pool holds it.
	for metric, p := range map[string]float64{"suite_duration_p95_seconds": 0.95, "suite_duration_p50_seconds": 0.5} {
		mustEqual(t, "control "+metric, e.value(org, metric, "", "2026-01-01", "2026-01-06"), "total", inclusive(floats(10, 20, 30, 40, 50, 600), p))
	}
	e.exec(`INSERT INTO testops_test_metrics_daily (org_id, day, repo_id, computed_at, retracted, team_id, service_id)
VALUES (?, '2026-01-03', ?, now() + INTERVAL 2 HOUR, 1, NULL, NULL)`, org, repoB)
	check("retired-present")
}
