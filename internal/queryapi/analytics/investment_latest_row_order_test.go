package analytics

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
)

func TestInvestmentReadersShareTheLatestRowOrder(t *testing.T) {
	sources := map[string]string{
		"LatestWorkUnitInvestmentsSource": LatestWorkUnitInvestmentsSource(),
	}
	for name, sql := range sources {
		if !strings.Contains(sql, latestrow.OrderKey) {
			t.Errorf("%s does not order by latestrow.OrderKey", name)
		}
		if strings.Contains(sql, ", computed_at)") {
			t.Errorf("%s orders a column by computed_at alone: %s", name, sql)
		}
	}
}
