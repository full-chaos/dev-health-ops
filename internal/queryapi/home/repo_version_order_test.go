package home

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
)

// Every metric spec of the registry builds its FROM clause (a spec on a table with no version order fails here, named,
// not as a request-time panic), and every metric of one table picks its version by the table's one order, so the
// metrics of a response read one stored version of a tied key.
func TestEveryMetricOfATableTakesTheTablesOneVersionOrder(t *testing.T) {
	for _, spec := range metrics {
		for _, oneTeam := range []bool{false, true} {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("%s (%s, one team %v): the FROM clause does not build: %v", spec.Metric, spec.Table, oneTeam, r)
					}
				}()
				clause := metricFromClause(spec.Table, spec.Column, "", "start_day", "end_day", oneTeam)
				if order := rowview.VersionOrder(spec.Table); !strings.Contains(clause, order) {
					t.Errorf("%s (%s, one team %v): the source does not pick by the table's one order %s:\n%s", spec.Metric, spec.Table, oneTeam, order, clause)
				}
			}()
		}
	}
}
