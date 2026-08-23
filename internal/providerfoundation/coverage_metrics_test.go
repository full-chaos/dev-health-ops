package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The two CHAOS-4130 counters ride the already-registered dev_health_provider_*
// family, which is what makes them scraped rather than constructed-and-
// discarded. Both carry a dataset label: a page cap or a destroyed unit is
// only actionable when an operator can see WHICH dataset it belongs to.
func TestCoverageCountersRenderPerProviderAndDataset(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordInventoryPageCap("GitHub", "tests")
	metrics.RecordInventoryPageCap("github", "tests")
	metrics.RecordInventoryPageCap("github", "cicd")
	metrics.RecordUnitTerminalWithRows("gitlab", "tests")
	// Unbounded label sources must collapse, never mint a series.
	metrics.RecordUnitTerminalWithRows("mystery-provider", "org-4711/repo-with-a-very-long-identifier")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_provider_inventory_page_cap_total{provider="github",dataset="tests"} 2`,
		`dev_health_provider_inventory_page_cap_total{provider="github",dataset="cicd"} 1`,
		`dev_health_provider_unit_terminal_with_rows_total{provider="gitlab",dataset="tests"} 1`,
		`dev_health_provider_unit_terminal_with_rows_total{provider="other",dataset="other"} 1`,
		"# TYPE dev_health_provider_inventory_page_cap_total counter",
		"# TYPE dev_health_provider_unit_terminal_with_rows_total counter",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	// A nil registry is the no-metrics deployment; recording must stay safe.
	var absent *Metrics
	absent.RecordInventoryPageCap("github", "tests")
	absent.RecordUnitTerminalWithRows("github", "tests")
}

func TestMetricDatasetBoundsTheLabel(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"tests": "tests", "pr-reviews": "pr-reviews", "work_items": "work_items",
		"TESTS": "tests", " tests ": "tests",
		"": "other", "acme/api": "other", "tests;drop": "other",
		strings.Repeat("d", 33): "other",
	} {
		if got := metricDataset(input); got != want {
			t.Fatalf("metricDataset(%q)=%q, want %q", input, got, want)
		}
	}
}
