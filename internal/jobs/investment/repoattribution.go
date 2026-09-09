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
	RepoAttributionSourceTeam       RepoAttributionSource = "team"
	RepoAttributionSourceUnassigned RepoAttributionSource = "unassigned"
)

// RepoAttributionCounts partitions every component ONE run materialized by
// how (or whether) its repository was resolved. Own + Ancestor + Children +
// Team + Unassigned always sum to the run's Components.
type RepoAttributionCounts struct {
	Own, Ancestor, Children, Team, Unassigned int
}

// RepoAttributionCountsFromStats derives the closed 5-way partition from a
// completed run's Stats.
//
// # WHY "TEAM" IS ALWAYS A SUBSET OF THE CASCADE'S "UNASSIGNED" BUCKET
//
// allocateTeamOwnership (teamownership.go) only reaches its donor lookup --
// the branch stats.RepoOwnershipFallback counts -- once every earlier check
// in that function has already failed: result.Investment.RepoID is nil (no
// own-edges signal and no hierarchy-cascade inheritance set it), no
// repo-effort record carries a real allocation source (which rules out the
// PR/commit-churn own signal churnRepoForComponent feeds into
// ownRepoByComponent -- see hierarchycascade.go's doc comment on why that is
// an "own" signal), and no component node names a repository directly.
// RepoCascadeOwn, RepoCascadeAncestor and RepoCascadeChildren
// (materialize.go's pre-pass, :281-328) between them cover both ways a
// component's own-edges/churn signal or the hierarchy cascade sets
// record.Investment.RepoID -- so any component the team fallback allocated
// was never counted in any of those three buckets. RepoCascadeUnassigned -
// RepoOwnershipFallback can therefore never go negative, and
// Own+Ancestor+Children+Team+Unassigned always equals Components, the same
// invariant Stats' own doc comments already assert for each half of this
// split individually.
func RepoAttributionCountsFromStats(stats Stats) RepoAttributionCounts {
	return RepoAttributionCounts{
		Own:        stats.RepoCascadeOwn,
		Ancestor:   stats.RepoCascadeAncestor,
		Children:   stats.RepoCascadeChildren,
		Team:       stats.RepoOwnershipFallback,
		Unassigned: stats.RepoCascadeUnassigned - stats.RepoOwnershipFallback,
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
		{jobruntime.InvestmentRepoAttributionSourceTeam, counts.Team},
		{jobruntime.InvestmentRepoAttributionSourceUnassigned, counts.Unassigned},
	}
	for _, pair := range pairs {
		_ = observer.Collector.ObserveInvestmentRepoAttribution(pair.source, pair.count)
	}
}
