package jobruntime

import (
	"strings"
	"testing"
)

// TestReadinessGateOutcomesArePublishedBeforeTheyHappen pins the property both
// readiness counters exist for, which is not "the counters count".
//
// Both of the gate's interesting outcomes are SILENT by default. A withheld
// org writes no recommendation rows; a fail-open writes rows that are merely
// wrong. Neither produces an error anywhere, and for this family "no rows" is
// an ordinary healthy result — its job is to say nothing is wrong. So the only
// way either outcome becomes observable is a series that exists from boot.
func TestReadinessGateOutcomesArePublishedBeforeTheyHappen(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	fresh := collector.PrometheusText()
	for _, class := range recommendationsReadinessFailOpenClasses {
		want := `worker_recommendations_readiness_fail_open_total{class="` + class + `"} 0`
		if !strings.Contains(fresh, want) {
			t.Errorf("a collector that has never failed open must still publish %q — "+
				"a rate() rule cannot bind to a series that springs into being at "+
				"the moment it was supposed to catch", want)
		}
	}
	if !strings.Contains(fresh, "worker_recommendations_readiness_skipped_total 0") {
		t.Error("the skipped counter must publish at zero from boot, for the same reason")
	}

	if err := collector.ObserveRecommendationsReadinessFailOpen(
		RecommendationsReadinessFailOpenQuery); err != nil {
		t.Fatalf("observe fail-open: %v", err)
	}
	collector.ObserveRecommendationsReadinessSkipped()

	after := collector.PrometheusText()
	if !strings.Contains(after,
		`worker_recommendations_readiness_fail_open_total{class="query"} 1`) {
		t.Error("the fail-open was not counted under its class")
	}
	if !strings.Contains(after, "worker_recommendations_readiness_skipped_total 1") {
		t.Error("the skip was not counted")
	}

	// The unrelated classes must still read zero rather than vanishing — a
	// counter that drops its zero series once a sibling fires reintroduces
	// exactly the gap the boot-time publication closes.
	if !strings.Contains(after,
		`worker_recommendations_readiness_fail_open_total{class="timeout"} 0`) {
		t.Error("classes other than the one that fired must remain published at zero")
	}
}

// TestReadinessFailOpenClassSetIsClosed pins the rejection, not the acceptance.
//
// The class set is deliberately NOT a transliteration of the reference's
// `type(exc).__name__` label: Go error types are unbounded, so an open set
// would both explode cardinality and make zero-emission impossible. That only
// holds if an unrecognised class is refused rather than silently given its own
// series — which would be a label no dashboard selects, i.e. an observation
// recorded and still invisible.
func TestReadinessFailOpenClassSetIsClosed(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	// A plausible-looking Go type name is the realistic mistake here, not a
	// nonsense string: it is what a literal port of the Python label produces.
	if err := collector.ObserveRecommendationsReadinessFailOpen("*pgconn.PgError"); err == nil {
		t.Fatal("an unknown class must be rejected; accepting it would create a " +
			"series no alert selects, which is worse than not counting at all")
	}

	if strings.Contains(collector.PrometheusText(), "*pgconn.PgError") {
		t.Error("a rejected class must not leave a series behind")
	}
}
