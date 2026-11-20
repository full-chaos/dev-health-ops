package benchmarking

import (
	"regexp"
	"testing"
)

// The pass rate benchmark is a ratio of sums: passed cases over passed plus failed cases of the repo-days of a group.
// The stored pass_rate column divides by every case (skipped ones too) and holds 0 on a day where every case was
// skipped, so it is never read.
func TestTestPassRateBenchmarkIsPooledOverExecutedCases(t *testing.T) {
	definition := metricDefinitions["pass_rate"]
	if definition.Table != "testops_test_metrics_daily" {
		t.Errorf("pass_rate: table %q, want testops_test_metrics_daily", definition.Table)
	}
	if definition.Numerator != "passed_count" || definition.Denominator != "passed_count + failed_count" {
		t.Errorf("pass_rate: numerator %q, denominator %q", definition.Numerator, definition.Denominator)
	}
	if definition.ValueColumn != "" {
		t.Errorf("pass_rate: value column %q, want none", definition.ValueColumn)
	}
	if regexp.MustCompile(`\bpass_rate\b`).MatchString(definition.Numerator + definition.Denominator) {
		t.Error("pass_rate: reads the stored rate column")
	}
}
