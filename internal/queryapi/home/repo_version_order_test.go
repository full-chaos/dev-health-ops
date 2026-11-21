package home

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// Every metric of one repository-keyed table picks its version by the table's one order, so the metrics of a response
// read one stored version of a tied key.
func TestEveryMetricOfATableTakesTheTablesOneVersionOrder(t *testing.T) {
	for _, spec := range metrics {
		if spec.Table == "work_item_metrics_daily" || spec.Table == "work_item_state_durations_daily" {
			continue
		}
		order := rowview.VersionOrder(spec.Table)
		for _, oneTeam := range []bool{false, true} {
			clause := metricFromClause(spec.Table, spec.Column, "", "start_day", "end_day", oneTeam)
			if strings.Count(clause, order) != 1 {
				t.Errorf("%s (%s, one team %v): the source does not pick by the table's one order %s:\n%s", spec.Metric, spec.Table, oneTeam, order, clause)
			}
		}
	}
}
