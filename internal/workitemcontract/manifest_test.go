package workitemcontract

import (
	"slices"
	"strings"
	"testing"
)

func TestDestinationSelectorsExposeOnlyTheirOwnEligibilityTags(t *testing.T) {
	t.Parallel()
	// The two selections were identical until CHAOS-4194 and are now written
	// separately, which is the point of the exercise: the shared prefix is
	// asserted once, and each tag's OWN extra surfaces are named explicitly, so
	// adding a destination cannot silently join a list nobody proved it safe
	// for. A single `want` reused for both would have let the new GitHub-effect
	// surfaces slip into the Linear retry policy by construction.
	shared := []string{
		"ai_attribution", "estimate_coverage_metrics_daily",
		"investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "sprints", "work_item_cycle_times",
		"work_item_dependencies", "work_item_interactions",
		"work_item_metrics_daily", "work_item_reopen_events",
		"work_item_state_durations_daily", "work_item_team_attributions",
		"work_item_transitions", "work_item_user_metrics_daily", "work_items",
	}
	// GitHub effect construction owns two more: the Projects V2 route produces
	// board memberships and the `projects` catalogue row that makes their
	// destination resolvable.
	wantGitHub := append(append([]string{}, shared...),
		"project_membership_transitions", "projects")
	if got := GitHubEffectDestinations(); !slices.Equal(got, wantGitHub) {
		t.Fatalf("GitHub effect destinations = %v, want %v", got, wantGitHub)
	}
	// Linear expired-lease retry owns NONE of them. Linear's route produces
	// neither family and no retry-safety proof exists for either, so this list
	// deliberately stops where the proofs do.
	if got := LinearExpiredLeaseRetryDestinations(); !slices.Equal(got, shared) {
		t.Fatalf("Linear expired-lease retry destinations = %v, want %v", got, shared)
	}
	// The divergence asserted directly, not just implied by the two lists
	// above. Without this a future edit that quietly tagged both destinations
	// LinearExpiredLeaseRetry:true would only have to update one literal to go
	// green, and the policy would silently grow two surfaces nobody reasoned
	// about.
	for _, destination := range []string{"project_membership_transitions", "projects"} {
		if slices.Contains(LinearExpiredLeaseRetryDestinations(), destination) {
			t.Fatalf("%q entered the Linear expired-lease retry policy with no safety proof", destination)
		}
	}
	for _, destination := range Destinations() {
		if destination.GitHubEffect && !slices.Contains(GitHubEffectDestinations(), destination.Name) {
			t.Fatalf("GitHub effect tag for %q was not selected", destination.Name)
		}
		if destination.LinearExpiredLeaseRetry && !slices.Contains(LinearExpiredLeaseRetryDestinations(), destination.Name) {
			t.Fatalf("Linear expired-lease retry tag for %q was not selected", destination.Name)
		}
	}
}

func TestFamilyDatasetIdentityAndLinearStorageKeyAreDerived(t *testing.T) {
	t.Parallel()
	orderedWant := []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	}
	if got := FamilyDatasets(); !slices.Equal(got, orderedWant) {
		t.Fatalf("family datasets=%v want %v", got, orderedWant)
	}
	want := []string{
		"work-item-comments", "work-item-history", "work-item-labels",
		"work-item-projects", "work-items",
	}
	if got := LinearBackfillWorkItemDatasets(); !slices.Equal(got, want) {
		t.Fatalf("Linear backfill datasets = %v, want %v", got, want)
	}
	for _, dataset := range want {
		if !IsFamilyDataset(dataset) {
			t.Fatalf("family dataset %q was not recognized", dataset)
		}
		if !IsLinearBackfillWorkItemDatasetKey(strings.ReplaceAll(dataset, "-", "_")) {
			t.Fatalf("storage key for %q was not recognized", dataset)
		}
	}
	if IsFamilyDataset("work-item-social") || IsLinearBackfillWorkItemDatasetKey("work_item_social") {
		t.Fatal("unknown work-item alias was recognized")
	}
}
