package workitemcontract

import (
	"slices"
	"strings"
	"testing"
)

func TestDestinationSelectorsExposeOnlyTheirOwnEligibilityTags(t *testing.T) {
	t.Parallel()
	want := []string{
		"ai_attribution", "estimate_coverage_metrics_daily",
		"investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "sprints", "work_item_cycle_times",
		"work_item_dependencies", "work_item_interactions",
		"work_item_metrics_daily", "work_item_reopen_events",
		"work_item_state_durations_daily", "work_item_team_attributions",
		"work_item_transitions", "work_item_user_metrics_daily", "work_items",
	}
	if got := GitHubEffectDestinations(); !slices.Equal(got, want) {
		t.Fatalf("GitHub effect destinations = %v, want %v", got, want)
	}
	if got := LinearExpiredLeaseRetryDestinations(); !slices.Equal(got, want) {
		t.Fatalf("Linear expired-lease retry destinations = %v, want %v", got, want)
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
