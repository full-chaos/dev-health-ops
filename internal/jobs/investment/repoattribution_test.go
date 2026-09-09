package investment

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// TestRepoAttributionCountsFromStats_ExactPartition fixtures a run touching
// all five sources at once and asserts the invariant CHAOS-5458 exists to
// report: own + ancestor + children + team + unassigned always equals the
// run's Components, with no source's count silently dropped or double
// counted against another.
func TestRepoAttributionCountsFromStats_ExactPartition(t *testing.T) {
	stats := Stats{
		Components:            100,
		RepoCascadeOwn:        20,
		RepoCascadeAncestor:   15,
		RepoCascadeChildren:   10,
		RepoCascadeUnassigned: 55, // 100 - 20 - 15 - 10, the pre-team-split cascade bucket
		RepoOwnershipFallback: 12, // the NxM team-ownership fallback fired for 12 of the 55
	}

	got := RepoAttributionCountsFromStats(stats)

	want := RepoAttributionCounts{Own: 20, Ancestor: 15, Children: 10, Team: 12, Unassigned: 43}
	if got != want {
		t.Fatalf("RepoAttributionCountsFromStats(%+v) = %+v, want %+v", stats, got, want)
	}

	sum := got.Own + got.Ancestor + got.Children + got.Team + got.Unassigned
	if sum != stats.Components {
		t.Errorf("partition sums to %d, want components=%d", sum, stats.Components)
	}
}

// TestRepoAttributionCountsFromStats_ZeroRunIsAnExplicitZeroPartition pins the
// empty-org path (materialize.go's len(components)==0 early return): every
// field must read as an explicit zero, not an absent one, matching the
// all-fields-including-zero discipline the run's other telemetry (Stats'
// RepoOwnership* fields) already documents.
func TestRepoAttributionCountsFromStats_ZeroRunIsAnExplicitZeroPartition(t *testing.T) {
	got := RepoAttributionCountsFromStats(Stats{})
	want := RepoAttributionCounts{}
	if got != want {
		t.Fatalf("RepoAttributionCountsFromStats(zero Stats) = %+v, want all-zero %+v", got, want)
	}
}

// TestCollectorRepoAttributionObserver_RendersEverySourceIncludingZero closes
// the loop between this package's counts and the jobruntime collector, which
// are in different packages and can drift apart -- the exact class of bug
// writeTeamRepoOwnershipDerivation's own doc comment warns about: a counter
// recorded via Observe* and never rendered by PrometheusText(). It also
// pins that a source with count zero (ancestor here) still appears in the
// exposition, rather than silently disappearing.
func TestCollectorRepoAttributionObserver_RendersEverySourceIncludingZero(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}

	observer := CollectorRepoAttributionObserver{Collector: collector}
	observer.ObserveRepoAttribution(RepoAttributionCounts{
		Own: 7, Ancestor: 0, Children: 3, Team: 5, Unassigned: 2,
	})

	text := collector.PrometheusText()
	for _, want := range []string{
		`dev_health_investment_repo_attribution_total{source="own"} 7`,
		`dev_health_investment_repo_attribution_total{source="ancestor"} 0`,
		`dev_health_investment_repo_attribution_total{source="children"} 3`,
		`dev_health_investment_repo_attribution_total{source="team"} 5`,
		`dev_health_investment_repo_attribution_total{source="unassigned"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("PrometheusText() missing %q (explicit zeros are required)\ngot:\n%s", want, text)
		}
	}
}

// TestCollectorRepoAttributionObserver_NilCollectorIsTolerated matches
// NativeExecutor.Execute's unconditional call site: a run with no observer
// wired must not panic or error.
func TestCollectorRepoAttributionObserver_NilCollectorIsTolerated(t *testing.T) {
	observer := CollectorRepoAttributionObserver{}
	observer.ObserveRepoAttribution(RepoAttributionCounts{Own: 1})
}
