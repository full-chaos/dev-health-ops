package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveRemainingMetricsManualOverlapExposesTheDoraSeries pins CHAOS-9692's
// series: an automatic dora start whose window shares a day with a pending or
// running manual run. It is emitted from zero so a dashboard built before the
// first overlap resolves.
func TestObserveRemainingMetricsManualOverlapExposesTheDoraSeries(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if text := collector.PrometheusText(); !strings.Contains(text, `dev_health_remaining_metrics_manual_overlap_total{family="dora"} 0`+"\n") {
		t.Fatalf("the series is not emitted from zero:\n%s", text)
	}
	for i := 0; i < 2; i++ {
		if err := collector.ObserveRemainingMetricsManualOverlap("dora"); err != nil {
			t.Fatal(err)
		}
	}
	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_remaining_metrics_manual_overlap_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	if !strings.Contains(text, `dev_health_remaining_metrics_manual_overlap_total{family="dora"} 2`+"\n") {
		t.Fatalf("missing the counted series:\n%s", text)
	}
}

func TestObserveRemainingMetricsManualOverlapRejectsAnUnknownFamily(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsManualOverlap("capacity"); err == nil {
		t.Fatal("expected an error for a family the overlap counter does not cover")
	}
	if text := collector.PrometheusText(); !strings.Contains(text, `dev_health_remaining_metrics_manual_overlap_total{family="dora"} 0`+"\n") {
		t.Fatalf("a refused observation changed the series:\n%s", text)
	}
}
