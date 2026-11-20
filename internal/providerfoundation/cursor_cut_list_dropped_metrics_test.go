package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The cut-list drop counter has one bounded label: the dropped list.
func TestCursorCutListDroppedCounterRendersPerBoundedList(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordCursorCutListDropped("active")
	metrics.RecordCursorCutListDropped("active")
	metrics.RecordCursorCutListDropped("carried")
	metrics.RecordCursorCutListDropped("org-4711/some-path")
	var nilMetrics *Metrics
	nilMetrics.RecordCursorCutListDropped("active")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_github_tests_cursor_cut_list_dropped_total counter",
		`dev_health_github_tests_cursor_cut_list_dropped_total{list="active"} 2`,
		`dev_health_github_tests_cursor_cut_list_dropped_total{list="carried"} 1`,
		`dev_health_github_tests_cursor_cut_list_dropped_total{list="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "org-4711") {
		t.Fatalf("a free label reached the series:\n%s", rendered)
	}
}
