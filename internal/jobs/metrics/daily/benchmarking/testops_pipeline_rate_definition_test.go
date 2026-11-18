package benchmarking

import (
	"strings"
	"testing"
)

// The pipeline success and failure benchmarks read the counts over the completed runs, NULL on a row with no
// completed run; the stored success_rate and failure_rate columns hold 0 on such a day and are never read.
func TestPipelineRateBenchmarksReadCompletedRunCounts(t *testing.T) {
	for name, count := range map[string]string{"success_rate": "success_count", "failure_rate": "failure_count"} {
		definition := metricDefinitions[name]
		if definition.Table != "testops_pipeline_metrics_daily" {
			t.Errorf("%s: table %q", name, definition.Table)
		}
		want := "if(success_count + failure_count = 0, NULL, " + count + " / (success_count + failure_count))"
		if definition.ValueColumn != want {
			t.Errorf("%s: value %q, want %q", name, definition.ValueColumn, want)
		}
		if strings.Contains(definition.ValueColumn, name) {
			t.Errorf("%s: value %q reads the stored rate column", name, definition.ValueColumn)
		}
	}
	if metricDefinitions[CanonicalMetricName("pipeline_success")].ValueColumn != metricDefinitions["success_rate"].ValueColumn {
		t.Error("the pipeline_success alias does not read the success_rate definition")
	}
}

// The rerun benchmark is a ratio of sums over the repo-days of a group (reruns over runs), not the mean of the
// stored rates: the numerator is the stored rate times pipelines_count, the same as the query-api measure.
func TestRerunRateBenchmarkIsAPooledRatio(t *testing.T) {
	definition := metricDefinitions["rerun_rate"]
	if definition.Numerator != "rerun_rate * pipelines_count" || definition.Denominator != "pipelines_count" {
		t.Errorf("rerun_rate: numerator %q, denominator %q", definition.Numerator, definition.Denominator)
	}
	if definition.Table != "testops_pipeline_metrics_daily" {
		t.Errorf("rerun_rate: table %q", definition.Table)
	}
	for name, other := range metricDefinitions {
		if name != "rerun_rate" && (other.Numerator != "" || other.Denominator != "") {
			t.Errorf("%s: pooled by this change, which names rerun_rate alone", name)
		}
	}
}
