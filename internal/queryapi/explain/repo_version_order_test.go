package explain

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// Every metric spec of the registry builds its FROM clause (a spec on a table with no version order fails here, named,
// not as a request-time panic), and every metric of one table picks its version by the table's one order, so the
// metrics of an explain answer and of Home read one stored version of a tied key.
func TestEveryMetricOfATableTakesTheTablesOneVersionOrder(t *testing.T) {
	for metric, config := range metricConfigs {
		for _, oneTeam := range []bool{false, true} {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s (%s, one team %v): the FROM clause does not build: %v", metric, config.Table, oneTeam, r)
					}
				}()
				clause := metricFromClause(config.Table, config.Column, "", "start_day", "end_day", oneTeam)
				if order := rowview.VersionOrder(config.Table); !strings.Contains(clause, order) {
					t.Errorf("%s (%s, one team %v): the source does not pick by the table's one order %s:\n%s", metric, config.Table, oneTeam, order, clause)
				}
			}()
		}
	}
}
