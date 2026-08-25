package jobruntime

import (
	"strings"
	"testing"
)

// The compatibility bridge could only ever report a status code (CHAOS-4243):
// a partition that wrote real rows and a partition that reported success
// while writing nothing were indistinguishable from outside. These tests
// assert the numbers reach the EXPOSITION, not merely the struct -- mirrors
// TestDORAPartitionCountersReachTheExposition's rationale exactly.
func TestCompatibilityBridgePartitionCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	five := 5
	zero := 0
	if err := collector.ObserveCompatibilityPartition("release_impact", &five); err != nil {
		t.Fatalf("observe: %v", err)
	}
	// The zero-row completion the bridge could not previously distinguish
	// from a real write.
	if err := collector.ObserveCompatibilityPartition("release_impact", &zero); err != nil {
		t.Fatalf("observe: %v", err)
	}
	// nil means "this family's evidence carries no countable signal" -- must
	// not be conflated with an observed zero.
	if err := collector.ObserveCompatibilityPartition("membership_backfill", nil); err != nil {
		t.Fatalf("observe: %v", err)
	}

	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_remaining_bridge_rows_written_total{family="release_impact"} 5`,
		`worker_remaining_bridge_zero_row_partitions_total{family="release_impact"} 1`,
		// Emitted for every family in the closed set, including zeros, so the
		// series exists before it ever needs to move (same discipline as the
		// dora refusal-reason series).
		`worker_remaining_bridge_rows_written_total{family="membership_backfill"} 0`,
		`worker_remaining_bridge_zero_row_partitions_total{family="membership_backfill"} 0`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\nfull exposition:\n%s", want, exposition)
		}
	}
}

func TestCompatibilityBridgeRefusesUnknownFamily(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	zero := 0
	if err := collector.ObserveCompatibilityPartition("not_a_real_family", &zero); err == nil {
		t.Fatal("expected an error for an unknown family")
	}
}

func TestCompatibilityBridgeRefusesNegativeCounts(t *testing.T) {
	// Clamping a negative would fold a reporting bug into a plausible-looking
	// number. Refusing keeps the counters trustworthy.
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	negative := -1
	if err := collector.ObserveCompatibilityPartition("release_impact", &negative); err == nil {
		t.Fatal("expected an error for a negative row count")
	}
}
