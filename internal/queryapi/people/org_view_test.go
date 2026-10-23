package people

import (
	"strings"
	"testing"
)

// CHAOS-8863: the person metric reads of work_item_user_metrics_daily take the org view; the other tables
// (user_metrics_daily, work_item_cycle_times) hold no view column and get no predicate.
func TestLiveWhereOfUserMetricsTakesTheOrgViewAndOthersDoNot(t *testing.T) {
	const want = "(work_item_user_metrics_daily.row_view IN ('', 'org'))"
	if clause := liveWhere("work_item_user_metrics_daily"); !strings.Contains(clause, want) || !strings.Contains(clause, "retracted") {
		t.Errorf("liveWhere(work_item_user_metrics_daily) = %q, want the org view and the retraction rule", clause)
	}
	for _, table := range []string{"user_metrics_daily", "work_item_cycle_times"} {
		if strings.Contains(liveWhere(table), "row_view") {
			t.Errorf("liveWhere(%s) names row_view: %q", table, liveWhere(table))
		}
	}
}
