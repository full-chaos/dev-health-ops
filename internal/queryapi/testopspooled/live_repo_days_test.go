package testopspooled

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// CHAOS-14616: every pooled source takes its live gate from LiveRepoDays, which keeps a day with any live row at its newest
// stamp (liverow.NewestAnyLivePredicate) and not the argMax pick of one of the tied rows.
func TestLiveRepoDaysHoldsTheTieSafeRuleOnEveryPooledTable(t *testing.T) {
	t.Parallel()
	for _, table := range []string{
		"repo_metrics_daily", "deploy_metrics_daily", "incident_metrics_daily",
		"testops_pipeline_metrics_daily", "testops_test_metrics_daily", "cicd_metrics_daily",
	} {
		gate := LiveRepoDays(table)
		if want := "HAVING " + liverow.NewestAnyLivePredicate(table, "") + ")"; !strings.HasSuffix(gate, want) {
			t.Errorf("%s: LiveRepoDays does not end with %q:\n%s", table, want, gate)
		}
		if strings.Contains(gate, liverow.NewestPredicate(table, "")) {
			t.Errorf("%s: LiveRepoDays still holds the argMax pick", table)
		}
	}
}
