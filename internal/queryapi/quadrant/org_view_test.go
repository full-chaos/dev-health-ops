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
