// Package workitemcontract owns the small, provider-neutral contract shared by
// the work-item route and the expired-lease repair policy.
//
// The two consumers intentionally select their own eligibility tag. GitHub
// effect construction and Linear retry safety have independent correctness
// proofs, and since CHAOS-4194 the two selections genuinely DIVERGE: the
// project-membership and projects destinations are GitHub-effect surfaces with
// no Linear expired-lease retry proof behind them. That divergence is the
// design working, not a gap -- the alternative was a second hand-maintained
// list, or claiming a safety proof nobody made.
package workitemcontract

import (
	"slices"
	"strings"
)

// Destination declares one work-item persistence surface and the consumers
// that have independently proved it safe. The declaration order is canonical
// and deterministic; selectors preserve it.
type Destination struct {
	Name                    string
	GitHubEffect            bool
	LinearExpiredLeaseRetry bool
	// FamilyRoute marks a destination EVERY work-item provider's route writes.
	// It is what the published capability matrix advertises per provider, so a
	// destination only one provider writes must not carry it -- the matrix is a
	// cross-team contract, and listing a surface under gitlab that gitlab can
	// never write is a false claim other teams would build against.
	FamilyRoute bool
}

var destinationManifest = [...]Destination{
	{Name: "ai_attribution", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "estimate_coverage_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "investment_classifications_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "investment_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "issue_type_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	// CHAOS-4194. The FIRST two destinations where the two consumers diverge,
	// which is the case this package's two independent tags were built for.
	//
	// Placed in ALPHABETICAL position, not appended. The declaration order is
	// canonical and is the order effects are built and indexed in, and this list
	// has always been alphabetical -- so appending at the end silently moved
	// work_items from index 15 to 17 relative to anything that sorts the same
	// names, and an effect ledger written at one index was then read at another.
	// The invariant was never written down because nothing had tested it; it is
	// written down now.
	//
	// GitHubEffect: the GitHub Projects V2 route produces both -- board
	// memberships for pull requests, and the `projects` catalogue row that
	// makes their destination resolvable.
	//
	// LinearExpiredLeaseRetry: FALSE, and the falseness is the claim. Linear's
	// route produces neither family, and no expired-lease retry safety proof
	// exists for either; marking them true would assert a proof that has never
	// been made, on a policy whose whole job is deciding what is safe to redo
	// after a lease expires.
	//
	// FamilyRoute: FALSE. The published capability matrix advertises this set
	// per provider, and gitlab's "project" concept IS repo_id -- listing these
	// under it would publish a capability it cannot have.
	{Name: "project_membership_transitions", GitHubEffect: true, LinearExpiredLeaseRetry: false, FamilyRoute: false},
	{Name: "projects", GitHubEffect: true, LinearExpiredLeaseRetry: false, FamilyRoute: false},
	{Name: "sprints", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_cycle_times", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_dependencies", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_interactions", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_reopen_events", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_state_durations_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_team_attributions", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_transitions", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_item_user_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
	{Name: "work_items", GitHubEffect: true, LinearExpiredLeaseRetry: true, FamilyRoute: true},
}

// Destinations returns a defensive copy of the full semantic manifest. It is
// primarily useful to contract tests; production consumers should select only
// the tag whose safety invariant they enforce.
func Destinations() []Destination {
	return slices.Clone(destinationManifest[:])
}

// GitHubEffectDestinations returns every destination the complete GitHub
// work-item writer must evaluate, including destinations with zero rows.
func GitHubEffectDestinations() []string {
	return destinationNames(func(destination Destination) bool {
		return destination.GitHubEffect
	})
}

// FamilyRouteDestinations returns the destinations EVERY work-item provider's
// route writes, and therefore the set the published capability matrix
// advertises for gitlab, jira and linear.
//
// It stops short of GitHubEffectDestinations by exactly the two CHAOS-4194
// surfaces. GitLab must never write them -- GitLab's own "project" concept IS
// this schema's repo_id, which is why gitlab is not registered for the
// membership kind at all. Jira and Linear DO now write them too (CHAOS-4193,
// each from its own native producer: Jira's project-move changelog, Linear's
// issue history), but through a provider-specific append onto this shared
// base in execution_registry.go's Descriptor(), not through a manifest tag
// here -- this function stays the true "every work-item route writes these"
// floor, and would misrepresent gitlab's exclusion if it grew a Jira/Linear
// tag of its own.
func FamilyRouteDestinations() []string {
	return destinationNames(func(destination Destination) bool {
		return destination.FamilyRoute
	})
}

// LinearExpiredLeaseRetryDestinations returns only surfaces whose Linear
// expired-lease retry semantics have been proven safe.
func LinearExpiredLeaseRetryDestinations() []string {
	return destinationNames(func(destination Destination) bool {
		return destination.LinearExpiredLeaseRetry
	})
}

func destinationNames(include func(Destination) bool) []string {
	destinations := make([]string, 0, len(destinationManifest))
	for _, destination := range destinationManifest {
		if include(destination) {
			destinations = append(destinations, destination.Name)
		}
	}
	return destinations
}

var familyDatasets = [...]string{
	"work-items",
	"work-item-labels",
	"work-item-projects",
	"work-item-history",
	"work-item-comments",
}

// FamilyDatasets returns the five planner-collapsed work-item identities in
// their canonical declaration order. Callers that need the family contract
// must derive from this owner rather than re-declaring a provider or recovery
// policy list.
func FamilyDatasets() []string {
	return slices.Clone(familyDatasets[:])
}

// IsFamilyDataset reports whether dataset is one of the five planner-collapsed
// work-item identities. It does not decide whether a direct alias is runnable;
// that is an execution-admission concern.
func IsFamilyDataset(dataset string) bool {
	for _, candidate := range familyDatasets {
		if dataset == candidate {
			return true
		}
	}
	return false
}

// LinearBackfillWorkItemDatasets returns the planner identities in the stable
// lexical order the Python recovery oracle exposes.
func LinearBackfillWorkItemDatasets() []string {
	datasets := slices.Clone(familyDatasets[:])
	slices.Sort(datasets)
	return datasets
}

// IsLinearBackfillWorkItemDatasetKey reports the PostgreSQL sync-unit key
// form of a planner work-item identity. The storage spelling is derived rather
// than maintained as a second five-item map.
func IsLinearBackfillWorkItemDatasetKey(datasetKey string) bool {
	return IsFamilyDataset(strings.ReplaceAll(datasetKey, "_", "-"))
}
