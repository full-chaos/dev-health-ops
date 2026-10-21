package providersync

import (
	"slices"
	"testing"
)

// TestGitHubWorkItemMetricTripletOwnsExactlyThreeDerivedDestinations keeps the triplet inside its declared boundary:
// exactly three of the tables computed from stored rows, named here as literals so the test does not read the slice
// it checks. The daily job is their one writer, so none of the three is a destination of the work-items sync route.
func TestGitHubWorkItemMetricTripletOwnsExactlyThreeDerivedDestinations(t *testing.T) {
	want := []string{"work_item_cycle_times", "work_item_metrics_daily", "work_item_user_metrics_daily"}
	if !slices.Equal(githubWorkItemMetricTripletDestinations, want) {
		t.Fatalf("triplet destinations = %v, want %v", githubWorkItemMetricTripletDestinations, want)
	}
	for _, destination := range want {
		if !slices.Contains(githubWorkItemDerivedDestinations, destination) {
			t.Errorf("%q is not one of the route's derived destinations", destination)
		}
		if slices.Contains(githubWorkItemRouteDestinations(), destination) {
			t.Errorf("the daily-job table %q is a work-item route destination", destination)
		}
	}
}
