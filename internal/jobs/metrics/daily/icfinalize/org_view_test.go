package icfinalize

import (
	"strings"
	"testing"
)

// CHAOS-8863: the finalize merge sums a person's rows of every work scope into one user row, so its readback of
// work_item_user_metrics_daily takes the org view: with both views stored, every item would count in both.
func TestWorkItemReadbackTakesTheOrgView(t *testing.T) {
	if !strings.Contains(workItemMetricsSQL, "(work_item_user_metrics_daily.row_view IN ('', 'org'))") {
		t.Fatalf("the work item readback lacks the org view predicate:\n%s", workItemMetricsSQL)
	}
}
