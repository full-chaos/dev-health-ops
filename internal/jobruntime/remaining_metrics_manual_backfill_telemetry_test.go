package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveRemainingMetricsManualBackfillExposesByFamilyAndOutcome pins
// CHAOS-4254's alertable series: an operator-triggered manual backfill
// request, broken out by family and outcome so "started" activity can be
// told apart from "already_covered" refusals on the same dashboard.
func TestObserveRemainingMetricsManualBackfillExposesByFamilyAndOutcome(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("dora", "started"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("dora", "started"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("dora", "already_covered"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("complexity", "already_ran"); err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_remaining_metrics_manual_backfill_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	for _, want := range []string{
		`dev_health_remaining_metrics_manual_backfill_total{family="dora",outcome="started"} 2`,
		`dev_health_remaining_metrics_manual_backfill_total{family="dora",outcome="already_covered"} 1`,
		`dev_health_remaining_metrics_manual_backfill_total{family="dora",outcome="already_ran"} 0`,
		`dev_health_remaining_metrics_manual_backfill_total{family="complexity",outcome="already_ran"} 1`,
		// Every (family, outcome) pair is emitted from zero, including a
		// family that never had this outcome fire -- release_impact never
		// observed at all in this test still gets a zero series, so a
		// dashboard built before the first real invocation still resolves.
		`dev_health_remaining_metrics_manual_backfill_total{family="release_impact",outcome="started"} 0`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

func TestObserveRemainingMetricsManualBackfillRejectsUnknownFamilyAndOutcome(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("capacity", "started"); err == nil {
		t.Fatal("expected an error for a non-day-scoped family this counter does not track")
	}
	if err := collector.ObserveRemainingMetricsManualBackfill("dora", "not-a-real-outcome"); err == nil {
		t.Fatal("expected an error for an unregistered outcome")
	}
	text := collector.PrometheusText()
	// "capacity" alone would also match unrelated series this collector
	// already emits (worker_capacity_native_*) -- scope the check to this
	// metric's own name plus the rejected label values.
	if strings.Contains(text, `dev_health_remaining_metrics_manual_backfill_total{family="capacity"`) ||
		strings.Contains(text, "not-a-real-outcome") {
		t.Fatalf("a rejected observation leaked into the exposition text:\n%s", text)
	}
}
