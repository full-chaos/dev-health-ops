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
	workItem, _ := workItemScopeFilter("org", []string{"t1", "t2"}, nil)
	if !has(workItem) {
		t.Errorf("workItemScopeFilter of a team set lacks the org view predicate: %v", workItem)
	}
	// One team reads the team view (CHAOS-14376): no view predicate in the filter, the served team view as the source.
	if oneTeam, _ := workItemScopeFilter("org", []string{"t1"}, nil); has(oneTeam) {
		t.Errorf("workItemScopeFilter of one team holds the org view predicate: %v", oneTeam)
	}
	plain, _ := scopeFilter("org", []string{"t1"}, nil)
	if has(plain) {
		t.Errorf("scopeFilter (estimate coverage) holds a view predicate: %v", plain)
	}
}
