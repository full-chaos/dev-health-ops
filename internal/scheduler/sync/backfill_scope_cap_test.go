package sync

import (
	"fmt"
	"testing"
	"time"
)

// defaultUnitCap is SYNC_RUN_MAX_UNITS' default (NewNativeMaterializer).
const defaultUnitCap = 1000

// TestBackfillPlanSizeIsSourcesTimesDatasetsTimesChunks pins the arithmetic
// behind the unit cap: the plan of a whole GitHub integration (61 enabled
// repositories, ten plannable datasets) over a two-chunk window is over the
// default cap, and the same window scoped to a few sources and datasets (what
// `dho backfill run --source-ids --dataset-keys` writes to the trigger) is far
// under it. The materializer refuses a plan over the cap with ErrInvalidPlan.
func TestBackfillPlanSizeIsSourcesTimesDatasetsTimesChunks(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC) // two 7-day chunks
	var sources []PlanSource
	for index := 0; index < 61; index++ {
		name := fmt.Sprintf("acme/repo-%02d", index)
		sources = append(sources, PlanSource{ID: fmt.Sprintf("source-%02d", index), ExternalID: name, Provider: "github", FullName: name})
	}
	var datasets []PlanDataset
	for _, key := range []string{"repo-metadata", "commits", "commit-stats", "files", "blame", "cicd", "tests", "deployments", "security", "prs"} {
		datasets = append(datasets, PlanDataset{Key: key})
	}
	plan := func(sources []PlanSource, datasets []PlanDataset) int {
		t.Helper()
		units, err := BuildBackfillPlan(PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
			Since: &since, Before: &before, Sources: sources, Datasets: datasets,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(units)
	}
	unscoped := plan(sources, datasets)
	scoped := plan(sources[:3], datasets[:3])
	t.Logf("unscoped %d units, scoped %d units, cap %d", unscoped, scoped, defaultUnitCap)
	if unscoped <= defaultUnitCap {
		t.Fatalf("harness: the unscoped plan is %d units, not over the cap %d: nothing is measured", unscoped, defaultUnitCap)
	}
	if scoped == 0 || scoped > defaultUnitCap {
		t.Fatalf("the scoped plan is %d units, want between 1 and the cap %d", scoped, defaultUnitCap)
	}
}

// TestBackfillPlanDatasetScopeFitsALongWindowUnderTheCap is the bigboy shape: a
// GitHub integration with 33 enabled sources plans about ten units per source
// per 7-day chunk, so thirty days (five chunks) is over the cap; scoped to the
// two datasets prs and commits the same integration plans about two units per source
// per chunk, and a three-month window (thirteen chunks) fits.
func TestBackfillPlanDatasetScopeFitsALongWindowUnderTheCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	var sources []PlanSource
	for index := 0; index < 33; index++ {
		name := fmt.Sprintf("acme/repo-%02d", index)
		sources = append(sources, PlanSource{ID: fmt.Sprintf("source-%02d", index), ExternalID: name, Provider: "github", FullName: name})
	}
	var all []PlanDataset
	for _, key := range []string{"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "security",
		"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"} {
		all = append(all, PlanDataset{Key: key})
	}
	scoped := []PlanDataset{{Key: "prs"}, {Key: "commits"}}
	plan := func(datasets []PlanDataset, days int) int {
		t.Helper()
		since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		before := since.AddDate(0, 0, days)
		units, err := BuildBackfillPlan(PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
			Since: &since, Before: &before, Sources: sources, Datasets: datasets,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(units)
	}
	unscoped, narrowed := plan(all, 35), plan(scoped, 91)
	t.Logf("all datasets, 35 days: %d units; prs+commits, 91 days: %d units; cap %d", unscoped, narrowed, defaultUnitCap)
	if unscoped <= defaultUnitCap {
		t.Fatalf("harness: the unscoped plan is %d units, not over the cap %d: nothing is measured", unscoped, defaultUnitCap)
	}
	if narrowed == 0 || narrowed > defaultUnitCap {
		t.Fatalf("the dataset-scoped plan is %d units, want between 1 and the cap %d", narrowed, defaultUnitCap)
	}
}
