package capacityforecast

import (
	"strings"
	"testing"
)

// CHAOS-8863: the capacity reads of work_item_metrics_daily of no team or a team set take the org view (legacy rows
// and the org rows). One team reads the team view (CHAOS-14376): its conditions hold no view predicate, and its source
// is the served team view (the view pick of TeamPick). Both sources scope their own read by org_id (CHAOS-14406).
func TestCapacityScopeFiltersTakeTheOrgViewOfWorkItemMetrics(t *testing.T) {
	const want = "(work_item_metrics_daily.row_view IN ('', 'org'))"
	for name, teams := range map[string][]string{"no team": nil, "one team": {"t1"}, "several teams": {"t1", "t2"}} {
		conditions, _ := capacityScopeFilters("org", teams, nil)
		found := false
		for _, condition := range conditions {
			found = found || condition == want
		}
		if found != (len(teams) != 1) {
			t.Errorf("%s: the conditions %v hold the org view predicate = %v", name, conditions, found)
		}
		for _, newestDay := range []bool{false, true} {
			source := capacitySource(teams, "org_id = {org_id:String}", newestDay)
			if served := strings.Contains(source, "multiIf(work_item_metrics_daily.row_view"); served != (len(teams) == 1) {
				t.Errorf("%s: newest day %v: source %q is the served team view = %v", name, newestDay, source, served)
			}
			if !strings.Contains(source, "work_item_metrics_daily.org_id = {org_id:String}") {
				t.Errorf("%s: newest day %v: source %q is not scoped by org_id", name, newestDay, source)
			}
		}
	}
}
