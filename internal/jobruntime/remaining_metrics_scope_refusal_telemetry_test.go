package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveRemainingMetricsScopeRefusedExposesByFamilyAndReason pins
// CHAOS-5395's alertable series: a StartRunRequest scope
// normalizeStartRunRequest refused, broken out by family and bounded reason
// so "someone typo'd a dora metric name" is a distinct, dashboardable series
// from every other invalid-scope shape.
func TestObserveRemainingMetricsScopeRefusedExposesByFamilyAndReason(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsScopeRefused("dora", "unknown_dora_metric"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsScopeRefused("dora", "unknown_dora_metric"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsScopeRefused("capacity", "invalid_scope"); err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP dev_health_remaining_metrics_scope_refused_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	for _, want := range []string{
		`dev_health_remaining_metrics_scope_refused_total{family="dora",reason="unknown_dora_metric"} 2`,
		`dev_health_remaining_metrics_scope_refused_total{family="capacity",reason="invalid_scope"} 1`,
		// Every (family, reason) pair is emitted from zero, including a
		// family/reason pair that never fired in this test -- so a
		// dashboard built before the first real refusal still resolves.
		`dev_health_remaining_metrics_scope_refused_total{family="release_impact",reason="invalid_scope"} 0`,
		`dev_health_remaining_metrics_scope_refused_total{family="dora",reason="invalid_scope"} 0`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

func TestObserveRemainingMetricsScopeRefusedRejectsUnknownFamilyAndReason(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveRemainingMetricsScopeRefused("not-a-real-family", "invalid_scope"); err == nil {
		t.Fatal("expected an error for an unregistered family")
	}
	if err := collector.ObserveRemainingMetricsScopeRefused("dora", "not-a-real-reason"); err == nil {
		t.Fatal("expected an error for an unregistered reason")
	}
	text := collector.PrometheusText()
	if strings.Contains(text, "not-a-real-family") || strings.Contains(text, "not-a-real-reason") {
		t.Fatalf("a rejected observation leaked into the exposition text:\n%s", text)
	}
}
