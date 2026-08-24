package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveCoverageCacheInvalidationExposesEmittedAndConsumedPair pins the
// alertable pair CHAOS-4226 introduces: every finalize that decides to
// invalidate the coverage cache counts as emitted; only a Valkey-acknowledged
// write counts as consumed. Both series must exist for a provider as soon as
// one is emitted, so `emitted - consumed` is a number (not a missing series)
// from the first sample.
func TestObserveCoverageCacheInvalidationExposesEmittedAndConsumedPair(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveCoverageCacheInvalidation("github", true); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveCoverageCacheInvalidation("github", true); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveCoverageCacheInvalidation("gitlab", false); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, help := range []string{
		"# HELP devhealth_sync_coverage_cache_invalidations_emitted_total ",
		"# HELP devhealth_sync_coverage_cache_invalidations_consumed_total ",
	} {
		if !strings.Contains(text, help) {
			t.Fatalf("missing %q:\n%s", help, text)
		}
	}
	for _, want := range []string{
		`devhealth_sync_coverage_cache_invalidations_emitted_total{provider="github"} 2`,
		`devhealth_sync_coverage_cache_invalidations_consumed_total{provider="github"} 2`,
		`devhealth_sync_coverage_cache_invalidations_emitted_total{provider="gitlab"} 1`,
		`devhealth_sync_coverage_cache_invalidations_consumed_total{provider="gitlab"} 0`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

func TestObserveCoverageCacheInvalidationClampsUnknownProvider(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveCoverageCacheInvalidation("some-future-provider", true); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	if strings.Contains(text, "some-future-provider") {
		t.Fatalf("unknown provider leaked unclamped:\n%s", text)
	}
	want := `devhealth_sync_coverage_cache_invalidations_emitted_total{provider="unknown"} 1`
	if !strings.Contains(text, want+"\n") {
		t.Fatalf("missing clamped line %q:\n%s", want, text)
	}
}
