package report

import (
	"reflect"
	"testing"
)

// A pass rate chart bucket is its passed cases over its passed and failed cases, from the counts of
// testops_test_metrics_daily. A bucket with neither has no point. The stored pass_rate column divides by every case
// (skipped ones too) and holds 0 on a day where every case was skipped, so it is never charted.
func TestPassRateChartReadsPassedAndFailedCounts(t *testing.T) {
	rule, ok := chartRules["pass_rate"]
	if !ok {
		t.Fatal("pass_rate has no chart rule: the chart averages the stored pass_rate column")
	}
	if rule.table != "testops_test_metrics_daily" {
		t.Errorf("pass_rate rule table %q, want testops_test_metrics_daily", rule.table)
	}
	want := "if(sum(passed_count) + sum(failed_count) = 0, NULL, " +
		"toFloat64(sum(passed_count)) / toFloat64(sum(passed_count) + sum(failed_count)))"
	if rule.expression != want {
		t.Errorf("pass_rate rule expression %q, want %q", rule.expression, want)
	}
	if !reflect.DeepEqual(rule.inputs, []string{"passed_count", "failed_count"}) {
		t.Errorf("pass_rate rule inputs %v, want [passed_count failed_count]", rule.inputs)
	}
	definition := withChartRule(supportedMetrics["pass_rate"])
	if definition.rule != want || definition.SourceTable != "testops_test_metrics_daily" {
		t.Errorf("pass_rate chart definition: table %q rule %q, want the rule on testops_test_metrics_daily",
			definition.SourceTable, definition.rule)
	}
}
