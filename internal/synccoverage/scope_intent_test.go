package synccoverage

import (
	"strings"
	"testing"
)

// TestIntersectEnabledDatasetsKeepsOrderAndReportsExclusions pins the pure half
// of the CHAOS-4106 fix: a target-derived dataset list is narrowed to the rows
// the operator left enabled, and every dropped key is reported so the caller
// can log and count it rather than dropping it silently.
func TestIntersectEnabledDatasetsKeepsOrderAndReportsExclusions(t *testing.T) {
	targets := []string{"repo-metadata", "commits", "work-items", "work-item-labels", "prs"}
	enabled := []string{"prs", "commits", "repo-metadata", "security"}

	kept, excluded := intersectEnabledDatasets(targets, enabled)

	if strings.Join(kept, ",") != "repo-metadata,commits,prs" {
		t.Fatalf("kept = %v, want the enabled targets in target order", kept)
	}
	if strings.Join(excluded, ",") != "work-item-labels,work-items" {
		t.Fatalf("excluded = %v, want the disabled targets sorted", excluded)
	}
	// "security" is enabled but not target-derived; the intersection must not
	// invent it into a target-scoped config's coverage.
	for _, key := range kept {
		if key == "security" {
			t.Fatal("kept leaked a dataset the config's sync_targets never selected")
		}
	}
}

func TestIntersectEnabledDatasetsEmptyEnabledExcludesEverything(t *testing.T) {
	kept, excluded := intersectEnabledDatasets([]string{"commits", "prs"}, nil)
	if len(kept) != 0 {
		t.Fatalf("kept = %v, want empty", kept)
	}
	if strings.Join(excluded, ",") != "commits,prs" {
		t.Fatalf("excluded = %v, want both", excluded)
	}
}

// TestScopeIntentMetricsCountAndRender proves the telemetry the standing order
// requires is real: the counter moves on an exclusion and renders as a
// Prometheus counter fragment with a bounded provider label.
func TestScopeIntentMetricsCountAndRender(t *testing.T) {
	metrics := NewScopeIntentMetrics()
	metrics.observeExcluded("github", 5)
	metrics.observeExcluded("github", 2)
	metrics.observeExcluded("not-a-provider", 1)
	metrics.observeExcluded("github", 0)

	if got := metrics.excludedCount("github"); got != 7 {
		t.Fatalf("github counter = %d, want 7 (a zero-count observation must not move it)", got)
	}
	if got := metrics.excludedCount("not-a-provider"); got != 1 {
		t.Fatalf("unbounded provider counter = %d, want 1 under the 'other' label", got)
	}

	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	text := rendered.String()
	for _, want := range []string{
		"# TYPE dev_health_sync_coverage_datasets_excluded_by_intent_total counter",
		`dev_health_sync_coverage_datasets_excluded_by_intent_total{provider="github"} 7`,
		`dev_health_sync_coverage_datasets_excluded_by_intent_total{provider="other"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, text)
		}
	}
}
