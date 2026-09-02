package jobruntime

import (
	"strings"
	"testing"
	"time"
)

// The daily bridge's compatibility executor could only report a status
// code: a partition where a native family wrote real rows and one where it
// silently wrote nothing looked identical from outside. These tests assert
// the numbers reach the EXPOSITION, mirroring dora_telemetry_test.go for the
// daily bridge's per-family (not per-kind) shape (CHAOS-4276).
func TestDailyMetricsNativeFamilyCountersReachTheExposition(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	if err := collector.ObserveDailyMetricsNativeFamily(
		"team_wellbeing", DailyMetricsNativeFamilyOutcomeComputed, 12, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if err := collector.ObserveDailyMetricsNativeFamily(
		"team_wellbeing", DailyMetricsNativeFamilyOutcomeRefused, 0, 0,
	); err != nil {
		t.Fatalf("observe: %v", err)
	}

	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_daily_metrics_native_family_outcome_total{family="team_wellbeing",outcome="computed"} 1`,
		`worker_daily_metrics_native_family_outcome_total{family="team_wellbeing",outcome="refused"} 1`,
		`worker_daily_metrics_native_family_rows_written_total{family="team_wellbeing"} 12`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\nfull exposition:\n%s", want, exposition)
		}
	}
}

// TestDailyMetricsNativeFamilyAcceptsCICD pins cicd (CHAOS-4292) into the
// closed dailyMetricsNativeFamilies allowlist -- a family flipped to
// families.json's "go" without also being added here has EVERY
// ObserveDailyMetricsNativeFamily("cicd", ...) call refused with "daily
// metrics native family is not registered", silently losing the family's
// outcome/rows-written/duration series (found in review before merge).
func TestDailyMetricsNativeFamilyAcceptsCICD(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	if err := collector.ObserveDailyMetricsNativeFamily(
		"cicd", DailyMetricsNativeFamilyOutcomeComputed, 4, 25*time.Millisecond,
	); err != nil {
		t.Fatalf("observe cicd: %v", err)
	}
	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_daily_metrics_native_family_outcome_total{family="cicd",outcome="computed"} 1`,
		`worker_daily_metrics_native_family_rows_written_total{family="cicd"} 4`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\nfull exposition:\n%s", want, exposition)
		}
	}
}

// TestDailyMetricsNativeFamilyIncludesFileHotspotsFamilies is CHAOS-4277's
// telemetry registration proof (codex round 1 finding #2): dailyMetrics
// NativeFamilies is a CLOSED allow-list, and computeNativeFamilies
// (internal/jobs/metrics/daily/daily.go) discards ObserveDailyMetricsNative
// Family's error (`_ = handler.nativeObserver.Observe...`), so a family
// absent from this list silently loses ALL its telemetry -- both the
// "computed" and "refused" outcomes, forever, with no visible symptom short
// of reading this source file. Before this ticket's fix, "file_hotspots"
// and "file_risk_hotspots" were both absent.
func TestDailyMetricsNativeFamilyIncludesFileHotspotsFamilies(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	for _, family := range []string{"file_hotspots", "file_risk_hotspots"} {
		if err := collector.ObserveDailyMetricsNativeFamily(
			family, DailyMetricsNativeFamilyOutcomeComputed, 7, 5*time.Millisecond,
		); err != nil {
			t.Fatalf("%s must be a registered native family, got: %v", family, err)
		}
		exposition := collector.PrometheusText()
		want := `worker_daily_metrics_native_family_rows_written_total{family="` + family + `"} 7`
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing %q\nfull exposition:\n%s", want, exposition)
		}
	}
}

func TestDailyMetricsNativeFamilyRejectsUnregisteredFamily(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsNativeFamily(
		"not_a_registered_family", DailyMetricsNativeFamilyOutcomeComputed, 1, time.Millisecond,
	); err == nil {
		t.Fatal("an unregistered family must be refused")
	}
}

func TestDailyMetricsNativeFamilyRefusesNegativeCounts(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsNativeFamily(
		"team_wellbeing", DailyMetricsNativeFamilyOutcomeComputed, -1, time.Millisecond,
	); err == nil {
		t.Error("negative rowsWritten must be refused")
	}
	if err := collector.ObserveDailyMetricsNativeFamily(
		"team_wellbeing", DailyMetricsNativeFamilyOutcomeComputed, 1, -time.Millisecond,
	); err == nil {
		t.Error("negative duration must be refused")
	}
	if strings.Contains(collector.PrometheusText(), `family="team_wellbeing",outcome="computed"} 1`) {
		t.Error("a refused observation must not have been counted")
	}
}

// TestDailyMetricsNativeFamilyZeroSeriesExistBeforeAnyObservation proves the
// series exist at zero from process start, exactly like
// dailyMetricsFamilyZeroRowsWithSource: an operator's alert must be able to
// bind to the series before the first failure, not only after it.
func TestDailyMetricsNativeFamilyZeroSeriesExistBeforeAnyObservation(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	exposition := collector.PrometheusText()
	for _, want := range []string{
		`worker_daily_metrics_native_family_outcome_total{family="team_wellbeing",outcome="computed"} 0`,
		`worker_daily_metrics_native_family_outcome_total{family="team_wellbeing",outcome="refused"} 0`,
		`worker_daily_metrics_native_family_rows_written_total{family="team_wellbeing"} 0`,
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition is missing zero-value series %q", want)
		}
	}
}
