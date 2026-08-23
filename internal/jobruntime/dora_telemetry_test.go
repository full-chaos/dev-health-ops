package jobruntime

import (
	"strings"
	"testing"
)

// The native DORA counters exist because the HTTP compatibility bridge could
// only report a status code: a partition that computed nothing and one that
// computed everything looked identical from outside. These tests assert the
// numbers reach the EXPOSITION, not merely the struct -- a counter that is
// incremented and never written out is indistinguishable from no counter at
// all to everyone downstream of it.
func TestDORAPartitionCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	if err := collector.ObserveDORAPartition(14, 56, 3); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := collector.ObserveDORAPartition(7, 0, 0); err != nil {
		t.Fatalf("observe: %v", err)
	}

	exposition := collector.PrometheusText()
	for _, want := range []string{
		"worker_dora_native_partitions_total 2",
		"worker_dora_native_days_total 21",
		"worker_dora_native_rows_written_total 56",
		"worker_dora_native_skipped_rows_total 3",
		// The second partition ran and wrote nothing. That is the shape a
		// broken cutover takes, and it has its own counter because a
		// rows-written total alone would merely flatten -- which reads exactly
		// like low traffic.
		"worker_dora_native_empty_partitions_total 1",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q", want)
		}
	}
}

func TestDORAPartitionRefusesNegativeCounts(t *testing.T) {
	// Clamping a negative would fold a reporting bug into a plausible-looking
	// number. The executor ignores this error, so refusing costs nothing and
	// keeps the counters trustworthy.
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	for _, bad := range [][3]int{{-1, 0, 0}, {0, -1, 0}, {0, 0, -1}} {
		if err := collector.ObserveDORAPartition(bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("counts %v must be refused", bad)
		}
	}
	if strings.Contains(collector.PrometheusText(), "worker_dora_native_partitions_total 1") {
		t.Error("a refused observation must not have been counted")
	}
}

func TestDORARefusalIsAPositiveSignal(t *testing.T) {
	// Absence is not a signal. worker_dora_native_partitions_total flat at
	// zero means "unproven", not "healthy", and no alert can tell a refused
	// executor from a quiet day. Every reason is therefore emitted even at
	// zero, so a rate() rule exists BEFORE the first failure rather than
	// springing into being at the moment it was meant to catch.
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	fresh := collector.PrometheusText()
	for _, reason := range doraRefusalReasons {
		want := `worker_dora_native_refused_total{reason="` + reason + `"} 0`
		if !strings.Contains(fresh, want) {
			t.Errorf("a never-refused collector must still publish %q", want)
		}
	}

	if err := collector.ObserveDORARefused(DORARefusedOrderingContractMismatch); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if !strings.Contains(collector.PrometheusText(),
		`worker_dora_native_refused_total{reason="ordering_contract_mismatch"} 1`) {
		t.Error("the refusal was not counted under its reason")
	}

	// The reason set is closed: an unbounded reason string would be unbounded
	// label cardinality.
	if err := collector.ObserveDORARefused("something-new"); err == nil {
		t.Error("an unknown reason must be refused, not given its own series")
	}
}

func TestCapacityCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if err := collector.ObserveCapacityPartition(5, 3, 2); err != nil {
		t.Fatalf("observe: %v", err)
	}
	exposition := collector.PrometheusText()
	for _, want := range []string{
		"worker_capacity_native_partitions_total 1",
		"worker_capacity_native_scopes_total 5",
		"worker_capacity_native_rows_written_total 3",
		// The skip counter is the point: Python tolerates a scope with no
		// history by logging and continuing, so without this a run that
		// forecast nothing looks like a run with nothing to forecast.
		"worker_capacity_native_skipped_scopes_total 2",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q", want)
		}
	}
	// Every reason published at zero, so an alert rule exists before the first
	// failure rather than appearing at the moment it was meant to catch.
	for _, reason := range capacityRefusalReasons {
		want := `worker_capacity_native_refused_total{reason="` + reason + `"} 0`
		if !strings.Contains(exposition, want) {
			t.Errorf("a never-refused collector must still publish %q", want)
		}
	}
	if err := collector.ObserveCapacityRefused("invented"); err == nil {
		t.Error("an unknown reason must be refused, not given its own series")
	}
	if err := collector.ObserveCapacityPartition(-1, 0, 0); err == nil {
		t.Error("negative counts must be refused")
	}
}
