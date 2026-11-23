package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The retraction fallback counter has two bounded labels: the provider and the fallback taken.
func TestReportRetractionLimitedCounterRendersPerBoundedProviderAndReason(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordReportRetractionLimited("github", "one_row_per_slot")
	metrics.RecordReportRetractionLimited("github", "one_row_per_slot")
	metrics.RecordReportRetractionLimited("github", "not_retired")
	metrics.RecordReportRetractionLimited("org-4711-provider", "run 43600014259")
	var nilMetrics *Metrics
	nilMetrics.RecordReportRetractionLimited("github", "one_row_per_slot")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_report_retraction_limited_total counter",
		`dev_health_provider_report_retraction_limited_total{provider="github",reason="one_row_per_slot"} 2`,
		`dev_health_provider_report_retraction_limited_total{provider="github",reason="not_retired"} 1`,
		`dev_health_provider_report_retraction_limited_total{provider="other",reason="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "org-4711") || strings.Contains(rendered, "43600014259") {
		t.Fatalf("a free label reached the series:\n%s", rendered)
	}
}
