package jobruntime

import (
	"strings"
	"testing"
)

// TestReportDedupGuardCountersReachTheExposition is the CHAOS-4140
// exposition-level guard, mirroring TestDORAPartitionCountersReachTheExposition:
// a counter incremented but never written out is indistinguishable from no
// counter at all to everyone downstream of it.
func TestReportDedupGuardCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	if err := collector.ObserveReportDedupGuard(
		"dora_metrics_daily", ReportDedupReasonRetryGeneration, 120, 40,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}
	// A second call for a different table accumulates under its own key, not
	// blended into dora_metrics_daily's.
	if err := collector.ObserveReportDedupGuard(
		"cicd_metrics_daily", ReportDedupReasonRetryGeneration, 10, 0,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}
	// A second call for the SAME table accumulates.
	if err := collector.ObserveReportDedupGuard(
		"dora_metrics_daily", ReportDedupReasonRetryGeneration, 30, 5,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}

	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_report_dedup_guard_observed_rows_total{table="dora_metrics_daily",reason="retry_generation"} 150`,
		`worker_report_dedup_guard_skipped_rows_total{table="dora_metrics_daily",reason="retry_generation"} 45`,
		// skipped==0 is still emitted -- it is informative (this table's
		// queried key range currently carries no duplicate generation), not
		// an absent series.
		`worker_report_dedup_guard_observed_rows_total{table="cicd_metrics_daily",reason="retry_generation"} 10`,
		`worker_report_dedup_guard_skipped_rows_total{table="cicd_metrics_daily",reason="retry_generation"} 0`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\ngot:\n%s", want, exposition)
		}
	}
}

// TestReportDedupGuardIsAbsentUntilFirstObservation is the counterpoint to
// the DORA-refusal "emit every reason at zero" pattern: the (table, reason)
// key set is not statically known to jobruntime (internal/jobs/report owns
// appendOnlyDailyKeys), so unlike doraRefusalReasons there is no closed list
// to pre-populate. A fresh collector must publish neither metric name at
// all, and the first observation must bring both up together.
func TestReportDedupGuardIsAbsentUntilFirstObservation(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	fresh := collector.PrometheusText()
	if strings.Contains(fresh, "worker_report_dedup_guard_observed_rows_total") ||
		strings.Contains(fresh, "worker_report_dedup_guard_skipped_rows_total") {
		t.Error("a never-observed collector must not publish the dedup guard series")
	}

	if err := collector.ObserveReportDedupGuard(
		"dora_metrics_daily", ReportDedupReasonRetryGeneration, 1, 0,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}
	exposition := collector.PrometheusText()
	if !strings.Contains(exposition, `worker_report_dedup_guard_observed_rows_total{table="dora_metrics_daily",reason="retry_generation"} 1`) {
		t.Error("observed_rows must appear after the first observation")
	}
	if !strings.Contains(exposition, `worker_report_dedup_guard_skipped_rows_total{table="dora_metrics_daily",reason="retry_generation"} 0`) {
		t.Error("skipped_rows must appear alongside observed_rows even when zero")
	}
}

func TestReportDedupGuardRefusesInvalidCounts(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	cases := []struct {
		name           string
		table, reason  string
		observed, skip int
	}{
		{"negative observed", "dora_metrics_daily", ReportDedupReasonRetryGeneration, -1, 0},
		{"negative skipped", "dora_metrics_daily", ReportDedupReasonRetryGeneration, 0, -1},
		{"skipped exceeds observed", "dora_metrics_daily", ReportDedupReasonRetryGeneration, 2, 3},
		{"empty table", "", ReportDedupReasonRetryGeneration, 1, 0},
		{"unknown reason", "dora_metrics_daily", "something-new", 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := collector.ObserveReportDedupGuard(tc.table, tc.reason, tc.observed, tc.skip); err == nil {
				t.Errorf("case %q must be refused", tc.name)
			}
		})
	}
	if strings.Contains(collector.PrometheusText(), "worker_report_dedup_guard_observed_rows_total") {
		t.Error("no refused observation may have been counted")
	}
}
