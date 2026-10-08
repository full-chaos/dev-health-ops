package analytics

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
)

func TestInvestmentReadersShareTheLatestRowOrder(t *testing.T) {
	sources := map[string]string{
		"LatestWorkUnitInvestmentsSource": LatestWorkUnitInvestmentsSource(),
		"windowedUnitEvidenceSource":      windowedUnitEvidenceSource(),
	}
	for name, sql := range sources {
		if !strings.Contains(sql, latestrow.OrderKey) {
			t.Errorf("%s does not order by latestrow.OrderKey", name)
		}
		if strings.Contains(sql, ", computed_at)") {
			t.Errorf("%s orders a column by computed_at alone: %s", name, sql)
		}
	}
	if want := latestrow.ArgMax("structural_evidence_json"); !strings.Contains(sources["windowedUnitEvidenceSource"], want) {
		t.Errorf("windowedUnitEvidenceSource must pick structural_evidence_json with %s", want)
	}
}
