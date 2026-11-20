package home

import "testing"

// A one-team request reads the team view of the team-keyed metrics only (CHAOS-14379). Every repository-scoped metric
// keeps the statement it had: the one-team gate is false for it, and even with the flag set its source is the same.
func TestOneTeamLeavesRepoScopedReadsUnchanged(t *testing.T) {
	oneTeam := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"P"}}}
	repoMetrics := 0
	for _, spec := range metrics {
		if spec.Scope != "repo" {
			continue
		}
		repoMetrics++
		if oneTeamScope(spec.Scope, oneTeam) {
			t.Errorf("%s: a repository metric takes the one-team gate", spec.Metric)
		}
		if _, dedup := dedupByComputedAt[spec.Table]; !dedup {
			continue
		}
		const filter = " AND repo_id IN {scope_ids:Array(String)}"
		if got, want := metricFromClause(spec.Table, spec.Column, filter, "start_day", "end_day", true), metricFromClause(spec.Table, spec.Column, filter, "start_day", "end_day", false); got != want {
			t.Errorf("%s: the one-team source changed a repository read:\n%s\nwant\n%s", spec.Metric, got, want)
		}
		if got, want := driverDeltaSource(spec.Table, spec.Column, filter, "start_day", "end_day", true), driverDeltaSource(spec.Table, spec.Column, filter, "start_day", "end_day", false); got != want {
			t.Errorf("%s: the one-team source changed a repository driver read", spec.Metric)
		}
	}
	if repoMetrics == 0 {
		t.Fatal("no repository metric in the spec list")
	}
}
