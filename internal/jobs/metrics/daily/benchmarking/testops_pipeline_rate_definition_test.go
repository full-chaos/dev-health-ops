package benchmarking

import (
	"strings"
	"testing"
)

// The pipeline success and failure benchmarks are a ratio of sums over the completed runs of the repo-days of a group;
// the stored success_rate and failure_rate columns hold 0 on a day with no completed run and are never read.
func TestPipelineRateBenchmarksArePooledOverCompletedRuns(t *testing.T) {
	for name, count := range map[string]string{"success_rate": "success_count", "failure_rate": "failure_count"} {
		definition := metricDefinitions[name]
		if definition.Table != "testops_pipeline_metrics_daily" {
			t.Errorf("%s: table %q", name, definition.Table)
		}
		if definition.Numerator != count || definition.Denominator != "success_count + failure_count" {
			t.Errorf("%s: numerator %q, denominator %q", name, definition.Numerator, definition.Denominator)
		}
		if strings.Contains(definition.Numerator+definition.Denominator, name) {
			t.Errorf("%s: reads the stored rate column", name)
		}
	}
	if metricDefinitions[CanonicalMetricName("pipeline_success")].Numerator != metricDefinitions["success_rate"].Numerator {
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
		pooled := name == "rerun_rate" || name == "success_rate" || name == "failure_rate" || name == "pass_rate" || name == "flake_rate" || name == "retry_dependency_rate" || name == "line_coverage_pct" || name == "branch_coverage_pct" || name == "defect_intro_rate"
		if !pooled && (other.Numerator != "" || other.Denominator != "") {
			t.Errorf("%s: pooled, which only the rate metrics are", name)
		}
	}
}
