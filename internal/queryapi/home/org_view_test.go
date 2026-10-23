package home

import (
	"strings"
	"testing"
)

// CHAOS-8863: the work item metric source takes the org view and keeps row_view in its natural key, so a legacy
// retraction row never merges with an org row under argMax. The user metrics table keeps its key until its own reader moves.
func TestMetricFromClauseOfWorkItemMetricsTakesTheOrgView(t *testing.T) {
	clause := metricFromClause("work_item_metrics_daily", "items_completed", "AND team_id = {team_id:String}", "start_day", "end_day")
	if !strings.Contains(clause, "(work_item_metrics_daily.row_view IN ('', 'org'))") {
		t.Errorf("no org view predicate in the source: %s", clause)
	}
	if !strings.Contains(clause, "GROUP BY day, provider, work_scope_id, team_id, row_view") {
		t.Errorf("row_view is not in the group of the source: %s", clause)
	}
	durations := metricFromClause("work_item_state_durations_daily", "duration_hours", "", "start_day", "end_day")
	if !strings.Contains(durations, "(work_item_state_durations_daily.row_view IN ('', 'org'))") ||
		!strings.Contains(durations, "GROUP BY day, provider, work_scope_id, team_id, status, row_view") {
		t.Errorf("the state durations source does not take the org view with row_view in its group: %s", durations)
	}
	users := metricFromClause("work_item_user_metrics_daily", "items_completed", "", "start_day", "end_day")
	if !strings.Contains(users, "(work_item_user_metrics_daily.row_view IN ('', 'org'))") ||
		!strings.Contains(users, "GROUP BY day, provider, work_scope_id, user_identity, row_view") {
		t.Errorf("the user metrics source does not take the org view with row_view in its group: %s", users)
	}
	other := metricFromClause("repo_metrics_daily", "pr_first_review_p50_hours", "", "start_day", "end_day")
	if strings.Contains(other, "row_view") {
		t.Errorf("a table without the view column mentions row_view: %s", other)
	}
}
