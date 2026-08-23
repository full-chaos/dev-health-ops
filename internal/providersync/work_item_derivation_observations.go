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
