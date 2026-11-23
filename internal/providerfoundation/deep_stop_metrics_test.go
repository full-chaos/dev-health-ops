package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The deep-stop counter renders per bounded provider, dataset and listing; an unknown listing collapses to "other" and a nil
// Metrics is a no-op (CHAOS-14520).
func TestDeepListingStopCounterRendersPerBoundedListing(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordDeepListingStop("GitHub", "tests", "runs")
	metrics.RecordDeepListingStop("github", "tests", "runs")
	metrics.RecordDeepListingStop("github", "cicd", "reports")
	metrics.RecordDeepListingStop("github", "tests", "org-4711/some-path")
	var nilMetrics *Metrics
	nilMetrics.RecordDeepListingStop("github", "tests", "runs")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_deep_stop_total counter",
		`dev_health_provider_deep_stop_total{provider="github",dataset="tests",listing="runs"} 2`,
		`dev_health_provider_deep_stop_total{provider="github",dataset="cicd",listing="reports"} 1`,
		`dev_health_provider_deep_stop_total{provider="github",dataset="tests",listing="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "org-4711") {
		t.Fatalf("a free label reached the series:\n%s", rendered)
	}
}
