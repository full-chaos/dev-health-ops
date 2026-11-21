package explain

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// Every metric of one repository-keyed table picks its version by the table's one order, so the metrics of an
// explain answer and of Home read one stored version of a tied key.
func TestEveryMetricOfATableTakesTheTablesOneVersionOrder(t *testing.T) {
	for metric, config := range metricConfigs {
		if config.Table == "work_item_metrics_daily" || config.Table == "work_item_state_durations_daily" {
			continue
		}
		order := rowview.VersionOrder(config.Table)
		for _, oneTeam := range []bool{false, true} {
			clause := metricFromClause(config.Table, config.Column, "", "start_day", "end_day", oneTeam)
			if strings.Count(clause, order) != 1 {
				t.Errorf("%s (%s, one team %v): the source does not pick by the table's one order %s:\n%s", metric, config.Table, oneTeam, order, clause)
			}
		}
	}
}
