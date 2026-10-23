package benchmarking

import (
	"reflect"
	"testing"
)

// CHAOS-8863: the work item benchmarks take the org view of work_item_metrics_daily.
func TestWorkItemBenchmarksTakeTheOrgView(t *testing.T) {
	for _, name := range []string{"cycle_time_hours", "defect_intro_rate"} {
		definition := metricDefinitions[name]
		if !reflect.DeepEqual(definition.ExtraFilters, []string{"(work_item_metrics_daily.row_view IN ('', 'org'))"}) {
			t.Errorf("%s: extra filters %v", name, definition.ExtraFilters)
		}
		if definition.InnerGroupColumns[len(definition.InnerGroupColumns)-1] != "row_view" {
			t.Errorf("%s: row_view is not in the group %v", name, definition.InnerGroupColumns)
		}
	}
}
