package benchmarking

import (
	"regexp"
	"testing"
)

// The pass rate benchmark reads the counts: passed over passed plus failed, NULL on a row with neither. The stored
// pass_rate column divides by every case (skipped ones too) and holds 0 on a day where every case was skipped, so it
// is never read.
func TestTestPassRateBenchmarkReadsPassedAndFailedCounts(t *testing.T) {
	definition := metricDefinitions["pass_rate"]
	if definition.Table != "testops_test_metrics_daily" {
		t.Errorf("pass_rate: table %q, want testops_test_metrics_daily", definition.Table)
	}
	want := "if(passed_count + failed_count = 0, NULL, passed_count / (passed_count + failed_count))"
	if definition.ValueColumn != want {
		t.Errorf("pass_rate: value %q, want %q", definition.ValueColumn, want)
	}
	if regexp.MustCompile(`\bpass_rate\b`).MatchString(definition.ValueColumn) {
		t.Errorf("pass_rate: value %q reads the stored rate column", definition.ValueColumn)
	}
}
