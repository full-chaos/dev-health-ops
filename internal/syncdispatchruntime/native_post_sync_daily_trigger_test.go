package syncdispatchruntime

import "testing"

// TestDailyMetricsTriggerFiresOnEveryRelevantTarget is the CHAOS-4246
// regression guard: metrics.daily_partition must re-trigger on a sync that
// wrote cicd/deployments/incidents data, not just git/work-items. Before this
// change dailyMetricsTrigger (inlined as `git || hasWorkItems`) missed the
// three CI/deploy/incident-only cases below, so a day's cicd/deploy/incident
// families could be computed once -- before that day's sync had caught up --
// and never recomputed, even though the partition kept reporting succeeded.
//
// Table-driven and clause-isolated on purpose (AGENTS.md mutation-testing
// note: "mutate compound predicates clause by clause -- a wholesale mutation
// reported KILLED on a condition holding a wrong, unasserted clause"): each
// case below flips exactly one input from all-false, so a mutant that drops
// any single `||` clause is caught by the case that isolates it.
func TestDailyMetricsTriggerFiresOnEveryRelevantTarget(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                                                     string
		git, hasWorkItems, hasCICD, hasDeployments, hasIncidents bool
		want                                                     bool
	}{
		{"nothing synced", false, false, false, false, false, false},
		{"git only", true, false, false, false, false, true},
		{"work-items only", false, true, false, false, false, true},
		{"cicd only -- the CHAOS-4246 gap", false, false, true, false, false, true},
		{"deployments only -- the CHAOS-4246 gap", false, false, false, true, false, true},
		{"incidents only -- the CHAOS-4246 gap", false, false, false, false, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dailyMetricsTrigger(tc.git, tc.hasWorkItems, tc.hasCICD, tc.hasDeployments, tc.hasIncidents)
			if got != tc.want {
				t.Fatalf("dailyMetricsTrigger(git=%v, hasWorkItems=%v, hasCICD=%v, hasDeployments=%v, hasIncidents=%v) = %v, want %v",
					tc.git, tc.hasWorkItems, tc.hasCICD, tc.hasDeployments, tc.hasIncidents, got, tc.want)
			}
		})
	}
}
