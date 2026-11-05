package investment

// repoattribution.go wires CHAOS-5458's per-run repo-attribution telemetry.
//
// materialize.go's "investment repo attribution" log line already computes
// this partition every run; before this file, it was written to the log and
// nowhere else -- "is repo attribution degrading" could only be answered by
// grepping worker logs across a fleet, and even then the team-fallback half
// of the partition (CHAOS-5460) lived in a SEPARATE log line ("investment
// team repository fallback"), correlated only by run_id. This exposes the
// SAME partition as a single Prometheus counter.

import (
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// RepoAttributionSource is the closed vocabulary the
// dev_health_investment_repo_attribution_total counter's `source` label
// takes. Mirrors jobruntime.InvestmentRepoAttributionSource's string values
// as a distinct type in this package -- the codebase's standing convention
// (see jobruntime's TeamRepoOwnershipResolutionArm doc comment) for a label
// vocabulary a lower-level telemetry package must not import the producing
// package to share.
type RepoAttributionSource string

const (
	RepoAttributionSourceOwn        RepoAttributionSource = "own"
	RepoAttributionSourceAncestor   RepoAttributionSource = "ancestor"
	RepoAttributionSourceChildren   RepoAttributionSource = "children"
	RepoAttributionSourceUnassigned RepoAttributionSource = "unassigned"
)

// RepoAttributionCounts partitions every component ONE run materialized by
// how (or whether) its repository was resolved. Own + Ancestor + Children +
// Unassigned always sum to the run's Components. There is no team bucket: a
// unit with no repository evidence is unassigned (CHAOS-12949, chris: "Drop
// fallback"); a team's repositories are never a unit's repository.
type RepoAttributionCounts struct {
	Own, Ancestor, Children, Unassigned int
}

// RepoAttributionCountsFromStats derives the closed 4-way partition from a
// completed run's Stats.
func RepoAttributionCountsFromStats(stats Stats) RepoAttributionCounts {
	return RepoAttributionCounts{
		Own:        stats.RepoCascadeOwn,
		Ancestor:   stats.RepoCascadeAncestor,
		Children:   stats.RepoCascadeChildren,
		Unassigned: stats.RepoCascadeUnassigned,
	}
}

// RepoAttributionObserver receives one run's closed repo-attribution
// partition. A narrow interface, same pattern as remaining.MembershipObserver,
// so Materializer.Run and NativeExecutor.Execute stay free of the telemetry
// package's shape.
type RepoAttributionObserver interface {
	ObserveRepoAttribution(RepoAttributionCounts)
}

// CollectorRepoAttributionObserver adapts the metrics collector to
// RepoAttributionObserver -- same pattern as
// remaining.CollectorMembershipObserver. Every source is reported on every
// call, including zero, matching
// jobruntime.InvestmentRepoAttributionObserver's contract.
type CollectorRepoAttributionObserver struct {
	Collector *jobruntime.MetricsCollector
}

func (observer CollectorRepoAttributionObserver) ObserveRepoAttribution(counts RepoAttributionCounts) {
	if observer.Collector == nil {
		return
	}
	pairs := []struct {
		source jobruntime.InvestmentRepoAttributionSource
		count  int
	}{
		{jobruntime.InvestmentRepoAttributionSourceOwn, counts.Own},
		{jobruntime.InvestmentRepoAttributionSourceAncestor, counts.Ancestor},
		{jobruntime.InvestmentRepoAttributionSourceChildren, counts.Children},
		{jobruntime.InvestmentRepoAttributionSourceUnassigned, counts.Unassigned},
	}
	for _, pair := range pairs {
		_ = observer.Collector.ObserveInvestmentRepoAttribution(pair.source, pair.count)
	}
}
