package jobruntime

import (
	"strings"
	"testing"
)

// TestRecommendationsRefusalIsAPositiveSignal pins the property the counter
// exists for, which is NOT "the counter counts".
//
// The per-KIND refusal established for DORA means a recommendations fault
// leaves its five siblings registered and healthy — deliberate, but it makes
// the refusal silent. And recommendations is the quietest possible family to
// lose: its job is to say "nothing is wrong", so a refused executor writing no
// rows looks exactly like a week in which no team tripped a rule.
//
// Every reason is therefore published at zero from boot, so a rate() rule
// exists BEFORE the first failure instead of springing into being at the moment
// it was meant to catch.
func TestRecommendationsRefusalIsAPositiveSignal(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	fresh := collector.PrometheusText()
	for _, reason := range recommendationsRefusalReasons {
		want := `worker_recommendations_native_refused_total{reason="` + reason + `"} 0`
		if !strings.Contains(fresh, want) {
			t.Errorf("a never-refused collector must still publish %q — a series that "+
				"only appears once it fires cannot be alerted on beforehand", want)
		}
	}

	if err := collector.ObserveRecommendationsRefused(
		RecommendationsRefusedSchemaIncompatible); err != nil {
		t.Fatalf("observe: %v", err)
	}
	if !strings.Contains(collector.PrometheusText(),
		`worker_recommendations_native_refused_total{reason="schema_incompatible"} 1`) {
		t.Error("the refusal was not counted under its reason")
	}

	// The reason set is closed. An unknown reason would get its own series that
	// no dashboard selects, so the refusal would be recorded and still
	// invisible — worse than not recording it, because the counter looks healthy.
	if err := collector.ObserveRecommendationsRefused("something-new"); err == nil {
		t.Error("an unknown reason must be refused, not given its own series")
	}
}

// TestRecommendationsRefusalReasonsAreDistinguishable guards against the set
// collapsing to one catch-all.
//
// The three reasons carry different remedies: an unavailable connection is
// transient and self-heals, an incompatible schema needs a migration, and a
// failure to READ the schema is neither — it is an unreachable or
// permission-denied database that looks like a schema problem. Folding them
// together would leave the label present but useless.
func TestRecommendationsRefusalReasonsAreDistinguishable(t *testing.T) {
	if len(recommendationsRefusalReasons) < 3 {
		t.Fatalf("only %d refusal reasons; the set has collapsed and the label no "+
			"longer tells an operator what to do", len(recommendationsRefusalReasons))
	}
	seen := map[string]bool{}
	for _, reason := range recommendationsRefusalReasons {
		if seen[reason] {
			t.Errorf("duplicate refusal reason %q", reason)
		}
		seen[reason] = true
	}
}
