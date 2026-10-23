package explain

import (
	"strings"
	"testing"
)

// CHAOS-8863: the work item metric source of /explain takes the org view with row_view in its natural key.
func TestMetricFromClauseOfWorkItemMetricsTakesTheOrgView(t *testing.T) {
	clause := metricFromClause("work_item_metrics_daily", "items_completed", "AND team_id = {team_id:String}", "start_day", "end_day")
	if !strings.Contains(clause, "(work_item_metrics_daily.row_view IN ('', 'org'))") {
		t.Errorf("no org view predicate in the source: %s", clause)
	}
	if !strings.Contains(clause, "row_view") || !strings.Contains(clause, "GROUP BY day,\n        provider,\n        work_scope_id,\n        team_id,\n        row_view") {
		t.Errorf("row_view is not in the group of the source: %s", clause)
	}
}
