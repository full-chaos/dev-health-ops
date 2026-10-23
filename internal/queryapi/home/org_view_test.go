package home

import (
	"strings"
	"testing"
)

// CHAOS-8863: the work item metric source takes the org view and keeps row_view in its natural key, so a legacy
// retraction row never merges with an org row under argMax. The other work-item tables keep their keys until their
// own readers move.
func TestMetricFromClauseOfWorkItemMetricsTakesTheOrgView(t *testing.T) {
	clause := metricFromClause("work_item_metrics_daily", "items_completed", "AND team_id = {team_id:String}", "start_day", "end_day")
	if !strings.Contains(clause, "(work_item_metrics_daily.row_view IN ('', 'org'))") {
		t.Errorf("no org view predicate in the source: %s", clause)
	}
	if !strings.Contains(clause, "GROUP BY day, provider, work_scope_id, team_id, row_view") {
		t.Errorf("row_view is not in the group of the source: %s", clause)
	}
	other := metricFromClause("work_item_state_durations_daily", "duration_hours", "", "start_day", "end_day")
	if strings.Contains(other, "row_view") {
		t.Errorf("a state durations source mentions row_view before its own reader moves: %s", other)
	}
}
