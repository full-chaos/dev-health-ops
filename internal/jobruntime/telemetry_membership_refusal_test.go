package jobruntime

import (
	"strings"
	"testing"
)

// TestMembershipRefusalIsAPositiveSignal pins the property the counter exists
// for -- mirrors TestRecommendationsRefusalIsAPositiveSignal's rationale: a
// safety-net family that goes quiet (backlog rather than wrong data) needs a
// counter present from boot, so an alert can bind to it before the first
// failure, not after.
func TestMembershipRefusalIsAPositiveSignal(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	fresh := collector.PrometheusText()
	for _, reason := range membershipRefusalReasons {
		want := `worker_membership_backfill_native_refused_total{reason="` + reason + `"} 0`
		if !strings.Contains(fresh, want) {
			t.Errorf("a never-refused collector must still publish %q", want)
		}
	}

	if err := collector.ObserveMembershipRefused(MembershipRefusedWriterUnavailable); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if !strings.Contains(collector.PrometheusText(),
		`worker_membership_backfill_native_refused_total{reason="writer_unavailable"} 1`) {
		t.Error("the refusal was not counted under its reason")
	}

	if err := collector.ObserveMembershipRefused("something-new"); err == nil {
		t.Error("an unknown reason must be refused, not given its own series")
	}
}

// TestMembershipRunCountersReachTheExposition mirrors
// TestDORAPartitionCountersReachTheExposition's rationale: the numbers must
// reach the exposition, not merely the struct.
func TestMembershipRunCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if err := collector.ObserveMembershipRun(10, 7, 3, 42, 1, 2, 5); err != nil {
		t.Fatalf("observe: %v", err)
	}
	exposition := collector.PrometheusText()
	for _, want := range []string{
		"worker_membership_backfill_native_runs_total 1",
		"worker_membership_backfill_native_components_total 10",
		"worker_membership_backfill_native_matched_total 7",
		"worker_membership_backfill_native_skipped_total 3",
		"worker_membership_backfill_native_rows_written_total 42",
		"worker_membership_backfill_native_oversized_components_total 1",
		"worker_membership_backfill_native_dropped_edges_total 2",
		"worker_membership_backfill_native_dropped_nodes_total 5",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\nfull exposition:\n%s", want, exposition)
		}
	}
}

func TestMembershipRunRefusesNegativeCounts(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if err := collector.ObserveMembershipRun(-1, 0, 0, 0, 0, 0, 0); err == nil {
		t.Fatal("expected an error for a negative count")
	}
}
