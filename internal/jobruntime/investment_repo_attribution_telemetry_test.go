package jobruntime

import (
	"strings"
	"testing"
)

// TestObserveInvestmentRepoAttributionRecordsEveryTier (CHAOS-5459) pins the
// metric that closes the gap this ticket found: before it, Stats.RepoCascade*
// was computed on every investment.materialize run and then DISCARDED, so
// "how is repo attribution trending" could only be answered by an ad-hoc
// ClickHouse query against work_unit_repo_effort after the fact.
func TestObserveInvestmentRepoAttributionRecordsEveryTier(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentRepoAttribution("own_edges", 493); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentRepoAttribution("team_ownership", 767); err != nil {
		t.Fatal(err)
	}
	// A second run accumulates rather than replacing.
	if err := collector.ObserveInvestmentRepoAttribution("own_edges", 7); err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	for _, want := range []string{
		"# HELP dev_health_investment_repo_attribution_total ",
		`dev_health_investment_repo_attribution_total{source="own_edges"} 500`,
		`dev_health_investment_repo_attribution_total{source="team_ownership"} 767`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing exposition content %q:\n%s", want, text)
		}
	}
}

// TestObserveInvestmentRepoAttributionIsPreSeededAtZero: every tier's series
// must exist before the first run observes it. An absent series and a
// genuinely-zero one are indistinguishable to an alert, so "team_ownership
// stopped firing entirely" would otherwise read the same as "team_ownership
// fired zero times", which is exactly the class of silence this ticket exists
// to end.
func TestObserveInvestmentRepoAttributionIsPreSeededAtZero(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, source := range investmentRepoAttributionSources {
		want := `dev_health_investment_repo_attribution_total{source="` + source + `"} 0`
		if !strings.Contains(text, want) {
			t.Fatalf("missing pre-seeded series %q:\n%s", want, text)
		}
	}
}

// TestObserveInvestmentRepoAttributionRefusesUnknownInput is the NEGATIVE
// control for the two tests above: without it, an implementation that
// accepted anything would still pass them. It also pins that the label set is
// CLOSED -- an unbounded source string would let one malformed caller
// cardinality-explode the series.
func TestObserveInvestmentRepoAttributionRefusesUnknownInput(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentRepoAttribution("pr_churn", 1); err == nil {
		t.Error("an unregistered source was accepted; the label set must be closed " +
			"(pr_churn is an allocation_source, not an attribution TIER)")
	}
	if err := collector.ObserveInvestmentRepoAttribution("own_edges", -1); err == nil {
		t.Error("a negative count was accepted")
	}
	if strings.Contains(collector.PrometheusText(), `source="pr_churn"`) {
		t.Error("a refused source still reached the exposition")
	}
}
