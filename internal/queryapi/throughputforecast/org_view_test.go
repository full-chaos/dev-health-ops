package throughputforecast

import "testing"

// CHAOS-8863: the work item reads take the org view; estimate_coverage_metrics_daily has no view column and keeps
// scopeFilter.
func TestWorkItemScopeFilterTakesTheOrgViewAndScopeFilterDoesNot(t *testing.T) {
	const want = "(work_item_metrics_daily.row_view IN ('', 'org'))"
	has := func(conditions []string) bool {
		for _, condition := range conditions {
			if condition == want {
				return true
			}
		}
		return false
	}
	workItem, _ := workItemScopeFilter("org", []string{"t1"}, nil)
	if !has(workItem) {
		t.Errorf("workItemScopeFilter lacks the org view predicate: %v", workItem)
	}
	plain, _ := scopeFilter("org", []string{"t1"}, nil)
	if has(plain) {
		t.Errorf("scopeFilter (estimate coverage) holds a view predicate: %v", plain)
	}
}
