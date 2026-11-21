package explain

import "testing"

// A one-team request reads the team view of the team-keyed metrics only (CHAOS-14379). Every repository-scoped metric
// keeps the statement it had: with the one-team flag set, its source is the same as without it.
func TestOneTeamLeavesRepoScopedReadsUnchanged(t *testing.T) {
	repoMetrics := 0
	for name, config := range metricConfigs {
		if config.Scope != "repo" {
			continue
		}
		repoMetrics++
		const filter = " AND repo_id IN {scope_ids:Array(String)}"
		if got, want := metricFromClause(config.Table, config.Column, filter, "start_day", "end_day", true), metricFromClause(config.Table, config.Column, filter, "start_day", "end_day", false); got != want {
			t.Errorf("%s: the one-team source changed a repository read:\n%s\nwant\n%s", name, got, want)
		}
	}
	if repoMetrics == 0 {
		t.Fatal("no repository metric in the config list")
	}
}
