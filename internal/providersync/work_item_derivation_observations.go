package providersync

import "sync"

// workItemDerivationObservations carries what the team-inheritance donor load
// did on one provider unit, from the derivation context out to the route's
// result payload.
//
// It exists because providersync is a pure effect-ledger package: no logger,
// no metrics registry, and a Derive signature (map[string][]json.RawMessage)
// with nowhere to put a number that is not a destination row. Rather than
// widen that signature across four provider families and every test that
// calls it, each deriver holds a POINTER to one of these, allocated by its
// constructor. A pointer field survives the value receivers Derive uses, so
// the route can read what the derivation observed after the call returns.
//
// Lifetime is one provider unit: the production construction path builds a
// deriver per claim inside BuildExecutor (cmd/dev-health-worker), so counts
// accumulate across a unit's Derive calls (a chunked route emits several) and
// never bleed between units. The mutex guards that accumulation rather than
// any expected concurrency -- Derive is not called in parallel today, and this
// keeps it correct if that changes.
type workItemDerivationObservations struct {
	mu              sync.Mutex
	storedEdgeMerge githubWorkItemStoredEdgeMergeObservation
	// teamAttributionBySource tallies PRIMARY team_attribution rows this
	// DERIVATION RUN produced, keyed by winning source (CHAOS-4244). This is
	// PRE-WRITE DERIVATION VOLUME, not deduplicated persisted-row count
	// (codex, 2026-08-24, MEDIUM): the surrounding pipeline intentionally
	// re-emits a byte-identical attribution row every day a unit is
	// re-derived (the append-only-daily-tables contract -- collapse happens
	// only at write/readback via ReplacingMergeTree), so a three-day run
	// tallies three "written" primary rows for what settles to one eventual
	// stored row after dedup. Do not read this as "N rows now exist in
	// ClickHouse" -- it answers "how much attribution did THIS run compute",
	// which is still useful (chris's <=2% target on the residual is about
	// rate, not row cardinality) but is a different question. This package
	// has no metrics registry (see the type doc above), so it is the
	// "existing equivalent" of a written_total counter until a real one is
	// wired: the route attaches a snapshot onto the result's `observations`
	// map, which providerunit.Handler persists onto worker_job_runs --
	// queryable per provider/source.
	teamAttributionBySource map[string]int
}

func newWorkItemDerivationObservations() *workItemDerivationObservations {
	return &workItemDerivationObservations{}
}

// recordStoredEdgeMerge accumulates one derivation's stored-edge union counts.
// Nil-safe: a deriver built directly in a test carries no accumulator, and
// that must not change what the derivation itself does.
func (observations *workItemDerivationObservations) recordStoredEdgeMerge(
	observed githubWorkItemStoredEdgeMergeObservation,
) {
	if observations == nil {
		return
	}
	observations.mu.Lock()
	defer observations.mu.Unlock()
	observations.storedEdgeMerge.StoredEdgesMerged += observed.StoredEdgesMerged
	observations.storedEdgeMerge.DonorRescues += observed.DonorRescues
	observations.storedEdgeMerge.CrossProviderRescues += observed.CrossProviderRescues
}

func (observations *workItemDerivationObservations) storedEdgeMergeSnapshot() githubWorkItemStoredEdgeMergeObservation {
	if observations == nil {
		return githubWorkItemStoredEdgeMergeObservation{}
	}
	observations.mu.Lock()
	defer observations.mu.Unlock()
	return observations.storedEdgeMerge
}

// recordTeamAttributionRows accumulates one derivation's PRIMARY
// team_attribution rows by source -- pre-write derivation volume, not a
// deduplicated persisted-row count (see the field doc on
// teamAttributionBySource). Nil-safe for the same reason recordStoredEdgeMerge
// is: a deriver built directly in a test carries no accumulator.
func (observations *workItemDerivationObservations) recordTeamAttributionRows(
	rows []githubWorkItemTeamAttributionRow,
) {
	if observations == nil {
		return
	}
	observations.mu.Lock()
	defer observations.mu.Unlock()
	if observations.teamAttributionBySource == nil {
		observations.teamAttributionBySource = map[string]int{}
	}
	for _, row := range rows {
		if row.IsPrimary != 1 {
			continue
		}
		observations.teamAttributionBySource[row.Source]++
	}
}

func (observations *workItemDerivationObservations) teamAttributionBySourceSnapshot() map[string]int {
	if observations == nil {
		return map[string]int{}
	}
	observations.mu.Lock()
	defer observations.mu.Unlock()
	snapshot := make(map[string]int, len(observations.teamAttributionBySource))
	for source, count := range observations.teamAttributionBySource {
		snapshot[source] = count
	}
	return snapshot
}

// workItemDerivationObserver is the read side the routes use. Every production
// deriver implements it (asserted at compile time below); a route reads
// through the assertion so a deriver double in a test simply reports zeroes
// instead of forcing every fake to grow a method it has nothing to say about.
type workItemDerivationObserver interface {
	StoredEdgeMergeObservation() githubWorkItemStoredEdgeMergeObservation
}

// workItemTeamInheritanceResultKey is the route-result key the stored-edge
// observation lands under, inside the existing `observations` map that
// providerunit.Handler persists onto the unit payload.
const workItemTeamInheritanceResultKey = "team_inheritance"

// attachWorkItemTeamInheritanceObservation records the derivation's
// stored-edge union on a route result, under the `observations` map
// providerunit.Handler already persists onto the unit payload.
//
// It attaches UNCONDITIONALLY, zeroes included: an absent key would mean "this
// build cannot see stored edges" and a zero means "the union ran and found
// nothing to rescue". Collapsing those two would make the CHAOS-3978 recovery
// unreadable on exactly the units that matter -- the ones with nothing to
// rescue are the evidence that the population is draining.
func attachWorkItemTeamInheritanceObservation(
	result map[string]any, deriver any,
) map[string]any {
	if result == nil {
		result = map[string]any{}
	}
	observations, _ := result["observations"].(map[string]any)
	if observations == nil {
		observations = map[string]any{}
	}
	observations[workItemTeamInheritanceResultKey] = workItemDerivationObservationOf(deriver)
	result["observations"] = observations
	return result
}

func workItemDerivationObservationOf(deriver any) githubWorkItemStoredEdgeMergeObservation {
	observer, ok := deriver.(workItemDerivationObserver)
	if !ok {
		return githubWorkItemStoredEdgeMergeObservation{}
	}
	return observer.StoredEdgeMergeObservation()
}

var (
	_ workItemDerivationObserver = (*GitHubWorkItemDeriver)(nil)
	_ workItemDerivationObserver = (*GitLabWorkItemDeriver)(nil)
	_ workItemDerivationObserver = (*JiraWorkItemDeriver)(nil)
	_ workItemDerivationObserver = (*LinearWorkItemDeriver)(nil)
)

// githubWorkItemTeamAttributionResultKey is the route-result key CHAOS-4244's
// written-by-source tally lands under, inside the same `observations` map
// attachWorkItemTeamInheritanceObservation writes to. Named "written" for the
// operator question it answers ("how much attribution did this run produce"),
// NOT a claim of deduplicated ClickHouse row cardinality -- see
// teamAttributionBySource's doc (codex, 2026-08-24, MEDIUM).
const githubWorkItemTeamAttributionResultKey = "team_attribution_written"

// githubWorkItemTeamAttributionObserver is the GitHub-only read side for the
// PRE-WRITE derivation-volume tally recorded in deriveForProvider (see
// teamAttributionBySource's doc for why this is not a persisted-row count).
// It is a separate interface (not folded into workItemDerivationObserver) so
// this stays scoped to the GitHub route -- GitLab/Jira/Linear derivers are
// untouched by CHAOS-4244 and do not need a matching method just to keep a
// shared interface satisfied.
type githubWorkItemTeamAttributionObserver interface {
	TeamAttributionWrittenObservation() map[string]int
}

// attachGitHubWorkItemTeamAttributionObservation records the pre-write
// derivation-volume tally on a route result, under the same `observations`
// map providerunit.Handler persists. Attaches unconditionally, zeroes
// included, for the same reason attachWorkItemTeamInheritanceObservation
// does: an absent key would read as "cannot see this run's attribution", not
// "attributed nothing".
func attachGitHubWorkItemTeamAttributionObservation(
	result map[string]any, deriver any,
) map[string]any {
	if result == nil {
		result = map[string]any{}
	}
	observations, _ := result["observations"].(map[string]any)
	if observations == nil {
		observations = map[string]any{}
	}
	tally := map[string]int{}
	if observer, ok := deriver.(githubWorkItemTeamAttributionObserver); ok {
		tally = observer.TeamAttributionWrittenObservation()
	}
	observations[githubWorkItemTeamAttributionResultKey] = tally
	result["observations"] = observations
	return result
}
