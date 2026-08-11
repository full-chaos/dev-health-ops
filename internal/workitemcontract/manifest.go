// Package workitemcontract owns the small, provider-neutral contract shared by
// the work-item route and the expired-lease repair policy.
//
// The two consumers intentionally select their own eligibility tag. They name
// the same destinations today, but GitHub effect construction and Linear retry
// safety have independent correctness proofs and may diverge without creating
// a second hand-maintained list.
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
}

var destinationManifest = [...]Destination{
	{Name: "ai_attribution", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "estimate_coverage_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "investment_classifications_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "investment_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "issue_type_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "sprints", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_cycle_times", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_dependencies", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_interactions", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_reopen_events", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_state_durations_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_team_attributions", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_transitions", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_item_user_metrics_daily", GitHubEffect: true, LinearExpiredLeaseRetry: true},
	{Name: "work_items", GitHubEffect: true, LinearExpiredLeaseRetry: true},
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
