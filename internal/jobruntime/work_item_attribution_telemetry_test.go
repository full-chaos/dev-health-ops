package jobruntime

import (
	"strings"
	"testing"
)

// TestWorkItemAttributionCountersReachTheExposition is CHAOS-5078 codex round
// 2 F4's proof: the collector has carried refusal/run/scope/rows counters for
// the native work item attribution backstop since CHAOS-3092 PR-B, and
// ObserveWorkItemAttributionRefused/Run were already called correctly, but
// nothing wrote them into PrometheusText -- so an operator reading /metrics
// could never see any of it, regardless of what actually ran. Mirrors
// dora_telemetry_test.go's TestDORAPartitionCountersReachTheExposition:
// assert the numbers reach the EXPOSITION, not merely the struct.
func TestWorkItemAttributionCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	if err := collector.ObserveWorkItemAttributionRun(true, 0, 0, 40, 12, false); err != nil {
		t.Fatalf("observe org-wide run: %v", err)
	}
	if err := collector.ObserveWorkItemAttributionRun(false, 3, 1, 9, 4, false); err != nil {
		t.Fatalf("observe scoped run: %v", err)
	}
	if err := collector.ObserveWorkItemAttributionRun(false, 2, 0, 0, 0, true); err != nil {
		t.Fatalf("observe noop run: %v", err)
	}
	if err := collector.ObserveWorkItemAttributionRefused(WorkItemAttributionRefusedWriterUnavailable); err != nil {
		t.Fatalf("observe refusal: %v", err)
	}

	exposition := collector.PrometheusText()
	for _, want := range []string{
		"worker_work_item_attribution_native_runs_total 3",
		"worker_work_item_attribution_native_org_wide_runs_total 1",
		"worker_work_item_attribution_native_scoped_runs_total 1",
		"worker_work_item_attribution_native_noop_runs_total 1",
		"worker_work_item_attribution_native_repo_scopes_total 5",
		"worker_work_item_attribution_native_project_scopes_total 1",
		"worker_work_item_attribution_native_items_seen_total 49",
		"worker_work_item_attribution_native_rows_written_total 16",
		`worker_work_item_attribution_native_refused_total{reason="writer_unavailable"} 1`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q", want)
		}
	}
}

// TestWorkItemAttributionRefusalIsAPositiveSignal proves every refusal reason
// is emitted even at zero (same reasoning as TestDORARefusalIsAPositiveSignal):
// absence is not a signal, and a rate() alert needs the series to exist
// before the first failure, not spring into being at the moment it fires.
func TestWorkItemAttributionRefusalIsAPositiveSignal(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	exposition := collector.PrometheusText()
	for _, reason := range workItemAttributionRefusalReasons {
		if !strings.Contains(exposition, `worker_work_item_attribution_native_refused_total{reason="`+reason+`"} 0`) {
			t.Errorf("exposition is missing zero-valued reason %q", reason)
		}
	}
}
