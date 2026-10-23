package capacityforecast

import "testing"

// CHAOS-8863: both capacity reads of work_item_metrics_daily take the org view (legacy rows and the org rows).
func TestCapacityScopeFiltersTakeTheOrgViewOfWorkItemMetrics(t *testing.T) {
	const want = "(work_item_metrics_daily.row_view IN ('', 'org'))"
	for name, teams := range map[string][]string{"no team": nil, "one team": {"t1"}, "several teams": {"t1", "t2"}} {
		conditions, _ := capacityScopeFilters("org", teams, nil)
		found := false
		for _, condition := range conditions {
			found = found || condition == want
		}
		if !found {
			t.Errorf("%s: the conditions %v do not hold the org view predicate", name, conditions)
		}
	}
}
