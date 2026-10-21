package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The near-ceiling counter has one bounded label: the ceiling's name.
func TestTreeWalkNearCeilingCounterRendersPerBoundedLimit(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordTreeWalkNearCeiling("dirs")
	metrics.RecordTreeWalkNearCeiling("DIRS")
	metrics.RecordTreeWalkNearCeiling("entries")
	metrics.RecordTreeWalkNearCeiling("org-4711/some-path")
	var nilMetrics *Metrics
	nilMetrics.RecordTreeWalkNearCeiling("dirs")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_github_tree_walk_near_ceiling_total counter",
		`dev_health_github_tree_walk_near_ceiling_total{limit="dirs"} 2`,
		`dev_health_github_tree_walk_near_ceiling_total{limit="entries"} 1`,
		`dev_health_github_tree_walk_near_ceiling_total{limit="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "org-4711") {
		t.Fatalf("a free label reached the series:\n%s", rendered)
	}
}
