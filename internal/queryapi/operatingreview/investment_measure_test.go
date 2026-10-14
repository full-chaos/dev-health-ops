package operatingreview

import (
	"context"
	"strings"
	"testing"
)

// CHAOS-9622 (D5999): the week's investment is the COUNT of the units, read through the one shared measure; the
// metrics carry measure-neutral names (<theme>_work, unit "work units"), never "effort".
func TestInvestmentSectionReadsTheCountMeasure(t *testing.T) {
	client := &fakeClient{responses: emptyScanners(1)}
	if _, err := fetchInvestment(context.Background(), client, "org-1", nil, day("2026-01-05"), day("2026-01-12")); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("%d statements, want the one theme read", len(client.statements))
	}
	if !strings.Contains(client.statements[0], "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0)) AS value") {
		t.Errorf("the investment read does not sum the COUNT:\n%s", client.statements[0])
	}
	if strings.Contains(client.statements[0], "toFloat64(sum(subcategory_kv.2 * effort_value") {
		t.Errorf("the investment read weighs by effort_value:\n%s", client.statements[0])
	}

	section := investmentSection(periodRows{}, periodRows{})
	for _, metric := range section.metrics {
		if !strings.HasSuffix(metric.key, "_work") || strings.Contains(metric.key, "effort") || metric.unit != "work units" {
			t.Errorf("metric %q unit %q: want <theme>_work and the unit work units", metric.key, metric.unit)
		}
	}
	if len(section.metrics) != 5 {
		t.Errorf("%d metrics, want the 5 canonical themes", len(section.metrics))
	}
}
