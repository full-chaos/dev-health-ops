package quadrant

import (
	"strings"
	"testing"
)

// CHAOS-8863: team axes of work_item_metrics_daily take the org view (each item once in the organization), and the
// per-repository axis, which groups by work scope, takes the scope view (each item once per scope).
func TestWorkItemMetricAxesTakeTheirViewOfWorkItemMetrics(t *testing.T) {
	const org = "(m.row_view IN ('', 'org'))"
	const scope = "(m.row_view IN ('', 'scope'))"
	for _, name := range []string{"throughput", "cycle_time", "wip"} {
		if !strings.Contains(TeamMetrics[name].WhereClause, org) {
			t.Errorf("team axis %s lacks the org view: %q", name, TeamMetrics[name].WhereClause)
		}
	}
	if !strings.Contains(RepoMetrics["wip"].WhereClause, scope) || strings.Contains(RepoMetrics["wip"].WhereClause, org) {
		t.Errorf("the per-repository wip axis must take the scope view only: %q", RepoMetrics["wip"].WhereClause)
	}
}

// CHAOS-8863: the person axes of work_item_user_metrics_daily sum a person's work scopes, so they take the org view.
func TestPersonAxesOfWorkItemUserMetricsTakeTheOrgView(t *testing.T) {
	const org = "(m.row_view IN ('', 'org'))"
	for _, name := range []string{"throughput", "cycle_time", "wip"} {
		spec := PersonMetrics[name]
		if !strings.Contains(spec.Table, "work_item_user_metrics_daily") {
			t.Fatalf("person axis %s no longer reads work_item_user_metrics_daily: %s", name, spec.Table)
		}
		if !strings.Contains(spec.WhereClause, org) {
			t.Errorf("person axis %s lacks the org view: %q", name, spec.WhereClause)
		}
	}
}
