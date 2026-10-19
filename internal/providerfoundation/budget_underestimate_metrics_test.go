package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// A finished unit that sent more requests than the request-count estimate it
// was admitted on is one count, by bounded provider, dataset and dimension.
func TestBudgetUnderestimateCounterRendersPerProviderDatasetAndDimension(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordBudgetUnderestimate("GitHub", "commits", "rest_core")
	metrics.RecordBudgetUnderestimate("github", "commits", "rest_core")
	metrics.RecordBudgetUnderestimate("jira", "work-items", "search")
	metrics.RecordBudgetUnderestimate("mystery", "org-4711/whatever", "a-dimension-nobody-declared")
	var nilMetrics *Metrics
	nilMetrics.RecordBudgetUnderestimate("github", "commits", "rest_core")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_budget_underestimate_total counter",
		`dev_health_provider_budget_underestimate_total{provider="github",dataset="commits",dimension="rest_core"} 2`,
		`dev_health_provider_budget_underestimate_total{provider="jira",dataset="work-items",dimension="search"} 1`,
		`dev_health_provider_budget_underestimate_total{provider="other",dataset="other",dimension="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_provider_budget_underestimate_total{"); got != 3 {
		t.Fatalf("series=%d want 3 in:\n%s", got, rendered)
	}
}
