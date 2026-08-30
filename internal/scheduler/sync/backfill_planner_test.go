package sync

import (
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

func TestBuildBackfillPlanRejectsNonBackfillMode(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{SyncModeIncremental, SyncModeFullResync, "bogus"} {
		_, err := BuildBackfillPlan(PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: mode, Now: time.Now(),
		})
		if !errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("BuildBackfillPlan(mode=%q) = %v, want ErrInvalidPlan", mode, err)
		}
	}
}

func TestBuildBackfillPlanRequiresSinceAndBefore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	since := now.AddDate(0, 0, -10)
	for _, test := range []struct {
		name   string
		since  *time.Time
		before *time.Time
	}{
		{name: "both nil"},
		{name: "since only", since: &since},
		{name: "before only", before: &now},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildBackfillPlan(PlannerInput{
				OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
				Since: test.since, Before: test.before,
			})
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("BuildBackfillPlan(%s) = %v, want ErrInvalidPlan", test.name, err)
			}
		})
	}
}

func TestBuildBackfillPlanRejectsInvertedOrEqualRange(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	for _, before := range []time.Time{now.AddDate(0, 0, -11), now.AddDate(0, 0, -10)} {
		since := now.AddDate(0, 0, -10)
		_, err := BuildBackfillPlan(PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
			Since: &since, Before: &before,
		})
		if !errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("BuildBackfillPlan(since=%s, before=%s) = %v, want ErrInvalidPlan", since, before, err)
		}
	}
}

// TestBuildBackfillPlanChunksNonFamilyDatasetIntoMultipleUnits proves the
// headline CHAOS-4602 acceptance shape: a config with no explicit scope and
// a real Jira "incidents" dataset plans MULTIPLE units for one backfill
// window, mirroring _backfill_windows' 7-day chunking (planner.py:1854)
// rather than the single watermark-derived window every other mode plans.
func TestBuildBackfillPlanChunksNonFamilyDatasetIntoMultipleUnits(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 20, 15, 0, 0, 0, time.UTC)
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
		Since: &since, Before: &before,
		Sources:  []PlanSource{{ID: "source", ExternalID: "PROJ", Provider: "jira", FullName: "PROJ"}},
		Datasets: []PlanDataset{{Key: "incidents"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 2026-08-01..2026-08-20 inclusive at 7-day chunks: [01-07],[08-14],[15-20]
	// = 3 chunks/units.
	if len(units) != 3 {
		t.Fatalf("len(units) = %d, want 3: %+v", len(units), units)
	}
	if !units[0].WindowStart.Equal(since) {
		t.Errorf("units[0].WindowStart = %s, want exact requested since %s", units[0].WindowStart, since)
	}
	if !units[len(units)-1].WindowEnd.Equal(before) {
		t.Errorf("units[last].WindowEnd = %s, want exact requested before %s", *units[len(units)-1].WindowEnd, before)
	}
	for _, unit := range units {
		if unit.Dataset != "incidents" || unit.Provider != "jira" || unit.Mode != SyncModeBackfill {
			t.Errorf("unexpected unit shape: %+v", unit)
		}
	}
}

// TestBuildBackfillPlanCollapsesWorkItemFamilyPerChunkWindow proves the
// atomic work-item family still collapses to ONE composite "work-items"
// unit -- but now once PER merged chunk window, not once total, and every
// family flag is stamped on every chunk (CHAOS-3606/CHAOS-4054
// unconditional stamping, unchanged from the scheduled planner).
func TestBuildBackfillPlanCollapsesWorkItemFamilyPerChunkWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
		Since: &since, Before: &before,
		Sources: []PlanSource{{ID: "source", ExternalID: "PROJ", Provider: "jira", FullName: "PROJ"}},
		Datasets: []PlanDataset{
			{Key: "work-items"}, {Key: "work-item-labels"}, {Key: "work-item-projects"},
			{Key: "work-item-history"}, {Key: "work-item-comments"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 2026-08-01..2026-08-10 inclusive at 7-day chunks: [01-07],[08-10] = 2.
	if len(units) != 2 {
		t.Fatalf("len(units) = %d, want 2: %+v", len(units), units)
	}
	for _, unit := range units {
		if unit.Dataset != canonicalWorkItemsDataset {
			t.Fatalf("unit.Dataset = %q, want canonical work-items: %+v", unit.Dataset, unit)
		}
		for _, dataset := range workitemcontract.FamilyDatasets() {
			flag := familyDatasetFlag(dataset)
			if !unit.ProcessorFlags[flag] {
				t.Errorf("chunk %+v missing family flag %q", unit, flag)
			}
		}
	}
	if !units[0].WindowStart.Equal(since) {
		t.Errorf("units[0].WindowStart = %s, want %s", units[0].WindowStart, since)
	}
	if !units[1].WindowEnd.Equal(before) {
		t.Errorf("units[1].WindowEnd = %s, want %s", *units[1].WindowEnd, before)
	}
}

// TestBuildBackfillPlanFoldsPRSocialOnlyForContributingMembers proves the
// non-atomic PR-social fold still only stamps the family flag for the
// dataset an org actually enabled, matching _build_fold_family_units: a
// prs-only selection must not fabricate pr-reviews/pr-comments flags.
func TestBuildBackfillPlanFoldsPRSocialOnlyForContributingMembers(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	// A range spanning exactly 7 calendar days (aligned to UTC midnight)
	// resolves to exactly one chunk -- unlike now-7h/-7d, which would straddle
	// two 7-day chunks purely from the truncated calendar-date span.
	since := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
		Since: &since, Before: &before,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "prs"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("len(units) = %d, want 1: %+v", len(units), units)
	}
	unit := units[0]
	if unit.Dataset != canonicalPRSocialDataset {
		t.Fatalf("unit.Dataset = %q, want %q", unit.Dataset, canonicalPRSocialDataset)
	}
	if !unit.ProcessorFlags["family_dataset_prs"] {
		t.Errorf("missing family_dataset_prs flag: %+v", unit.ProcessorFlags)
	}
	if unit.ProcessorFlags["family_dataset_pr_reviews"] || unit.ProcessorFlags["family_dataset_pr_comments"] {
		t.Errorf("fabricated a flag for a member the org never enabled: %+v", unit.ProcessorFlags)
	}
}

// TestBuildBackfillPlanUsesWiderChunkForLinearWorkItemFamily proves the
// Linear work-item family's 14-day (not 7-day) chunk width, mirroring
// _is_linear_work_item_family/_linear_backfill_max_window_days.
func TestBuildBackfillPlanUsesWiderChunkForLinearWorkItemFamily(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
		Since: &since, Before: &before,
		Sources:  []PlanSource{{ID: "source", ExternalID: "team", Provider: "linear", FullName: "team"}},
		Datasets: []PlanDataset{{Key: "work-items"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 2026-08-01..2026-08-20 inclusive at 14-day chunks: [01-14],[15-20] = 2,
	// NOT the 3 a 7-day chunk would produce.
	if len(units) != 2 {
		t.Fatalf("len(units) = %d, want 2 (14-day chunk): %+v", len(units), units)
	}
}

func TestBuildBackfillPlanSingleDayRangePlansOneUnit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	since := now.Add(-time.Hour)
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: now,
		Since: &since, Before: &now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "PROJ", Provider: "jira", FullName: "PROJ"}},
		Datasets: []PlanDataset{{Key: "incidents"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("len(units) = %d, want 1: %+v", len(units), units)
	}
	if !units[0].WindowStart.Equal(since) || !units[0].WindowEnd.Equal(now) {
		t.Fatalf("unit=%+v want [since=%s, before=%s]", units[0], since, now)
	}
}
