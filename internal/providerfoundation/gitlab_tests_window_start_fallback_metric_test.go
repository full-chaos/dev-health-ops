package providerfoundation

import (
	"strings"
	"testing"
)

func TestRecordGitLabTestsWindowStartFallbackCountsByBoundedDataset(t *testing.T) {
	t.Parallel()
	var nilMetrics *Metrics
	nilMetrics.RecordGitLabTestsWindowStartFallback("tests")
	metrics := NewMetrics()
	metrics.RecordGitLabTestsWindowStartFallback("tests")
	metrics.RecordGitLabTestsWindowStartFallback("tests")
	metrics.RecordGitLabTestsWindowStartFallback("not-a-dataset")
	var out strings.Builder
	if err := metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	family := "dev_health_provider_gitlab_tests_window_start_fallback_total"
	for _, want := range []string{
		"# TYPE " + family + " counter",
		family + `{provider="gitlab",dataset="tests"} 2`,
		family + `{provider="gitlab",dataset="` + MetricDatasetLabel("not-a-dataset") + `"} 1`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("exposition lacks %q", want)
		}
	}
}
