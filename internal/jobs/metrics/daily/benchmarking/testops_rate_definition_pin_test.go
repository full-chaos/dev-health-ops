package benchmarking

import (
	"reflect"
	"testing"
)

// The pipeline rate definitions are pinned field by field: they are the SQL text of the pooled rate (table, numerator,
// denominator, group columns), so a change of the shared pooled helper cannot move them unnoticed.
func TestPipelineRateDefinitionsAreUnchanged(t *testing.T) {
	want := map[string]MetricDefinition{
		"success_rate": {Table: "testops_pipeline_metrics_daily", Numerator: "success_count", Denominator: "success_count + failure_count"},
		"failure_rate": {Table: "testops_pipeline_metrics_daily", Numerator: "failure_count", Denominator: "success_count + failure_count"},
		"rerun_rate":   {Table: "testops_pipeline_metrics_daily", Numerator: "rerun_rate * pipelines_count", Denominator: "pipelines_count"},
	}
	for name, definition := range want {
		definition.ScopeSupport = testopsScopes()
		definition.InnerGroupColumns = []string{"repo_id"}
		if got := metricDefinitions[name]; !reflect.DeepEqual(got, definition) {
			t.Errorf("%s: definition %+v, want %+v", name, got, definition)
		}
	}
}
