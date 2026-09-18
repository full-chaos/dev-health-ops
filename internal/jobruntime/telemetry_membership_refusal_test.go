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

// TestObserveMembershipMarkerLagExceededReachesTheExposition is the
// direct-collector proof CollectorMembershipObserver's own tests do not
// give: those exercise ONLY the adapter with a fake Collector field,
// never MetricsCollector.ObserveMembershipMarkerLagExceeded itself or
// its two exposition lines. Pins: a fresh collector publishes both
// series pre-seeded at zero; each call increments the counter and
// OVERWRITES the gauge with the latest value (never accumulates it,
// unlike the counter); a negative lagSeconds clamps to zero rather than
// going negative in the exposition.
func TestObserveMembershipMarkerLagExceededReachesTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	fresh := collector.PrometheusText()
	for _, want := range []string{
		"worker_membership_backfill_native_marker_lag_alerts_total 0",
		"worker_membership_backfill_native_marker_lag_seconds 0",
	} {
		if !strings.Contains(fresh, want) {
			t.Errorf("a never-alerted collector must still publish %q\nfull exposition:\n%s", want, fresh)
		}
	}

	collector.ObserveMembershipMarkerLagExceeded(10800)
	exposition := collector.PrometheusText()
	for _, want := range []string{
		"worker_membership_backfill_native_marker_lag_alerts_total 1",
		"worker_membership_backfill_native_marker_lag_seconds 10800",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q after one alert\nfull exposition:\n%s", want, exposition)
		}
	}

	// A second, SMALLER lag: the counter accumulates, the gauge does not
	// -- it reflects only the most recently observed lag.
	collector.ObserveMembershipMarkerLagExceeded(3600)
	exposition = collector.PrometheusText()
	for _, want := range []string{
		"worker_membership_backfill_native_marker_lag_alerts_total 2",
		"worker_membership_backfill_native_marker_lag_seconds 3600",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q after a second, smaller alert\nfull exposition:\n%s", want, exposition)
		}
	}

	collector.ObserveMembershipMarkerLagExceeded(-5)
	exposition = collector.PrometheusText()
	if !strings.Contains(exposition, "worker_membership_backfill_native_marker_lag_seconds 0") {
		t.Errorf("a negative lagSeconds must clamp to 0 in the exposition, not go negative or panic\nfull exposition:\n%s", exposition)
	}
	if !strings.Contains(exposition, "worker_membership_backfill_native_marker_lag_alerts_total 3") {
		t.Errorf("a negative lagSeconds still counts as one more alert call\nfull exposition:\n%s", exposition)
	}
}
