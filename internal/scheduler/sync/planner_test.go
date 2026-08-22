package sync

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

func TestScheduledPlannerDerivesTheCanonicalWorkItemFamilyContract(t *testing.T) {
	t.Parallel()
	want := []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	}
	if got := workitemcontract.FamilyDatasets(); !slices.Equal(got, want) {
		t.Fatalf("work-item family contract=%v want %v", got, want)
	}
}

func TestBuildScheduledPlanRejectsBackfill(t *testing.T) {
	_, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill, Now: time.Now(),
	})
	if !errors.Is(err, ErrBackfillScheduled) {
		t.Fatalf("BuildScheduledPlan(backfill) = %v, want ErrBackfillScheduled", err)
	}
}

func TestBuildScheduledPlanCollapsesWorkItemFamilyAndUsesEarliestWatermark(t *testing.T) {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	labels := now.Add(-24 * time.Hour)
	items := now.Add(-48 * time.Hour)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "work-item-labels"}, {Key: "prs"}, {Key: "work-items"}},
		Watermarks: map[WatermarkKey]time.Time{
			{SourceID: "owner/repo", Dataset: "work-items"}:       items,
			{SourceID: "owner/repo", Dataset: "work-item-labels"}: labels,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 2 {
		t.Fatalf("len(units) = %d, want 2: %+v", len(units), units)
	}
	family := units[1]
	if family.Dataset != canonicalWorkItemsDataset || family.WindowStart == nil || !family.WindowStart.Equal(items) {
		t.Fatalf("family unit = %+v, want canonical work-items at earliest watermark", family)
	}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		flag := familyDatasetFlag(dataset)
		if !family.ProcessorFlags[flag] {
			t.Errorf("family flag %q missing from %+v", flag, family.ProcessorFlags)
		}
	}
	if !family.ProcessorFlags["sync_prs"] {
		t.Errorf("sync_prs missing from %+v", family.ProcessorFlags)
	}
}

func TestBuildScheduledPlanGitHubWorkItemFamilyKeepsAllFiveFlagsForColdAndCaughtUpWindows(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	before := now.Add(-time.Hour)
	items := now.Add(-48 * time.Hour)
	for _, test := range []struct {
		name       string
		datasets   []PlanDataset
		watermarks map[WatermarkKey]time.Time
		before     *time.Time
		wantStart  time.Time
	}{
		{
			name:      "cold start from an alias",
			datasets:  []PlanDataset{{Key: "work-item-labels"}},
			wantStart: now.AddDate(0, 0, -defaultInitialSyncDepthDays),
		},
		{
			name: "caught up sibling contributes no window",
			datasets: []PlanDataset{
				{Key: "work-items"}, {Key: "work-item-labels"},
			},
			watermarks: map[WatermarkKey]time.Time{
				{SourceID: "owner/repo", Dataset: "work-items"}:       items,
				{SourceID: "owner/repo", Dataset: "work-item-labels"}: now,
			},
			before:    &before,
			wantStart: items,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			units, err := BuildScheduledPlan(PlannerInput{
				OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
				Before: test.before,
				Sources: []PlanSource{{
					ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo",
				}},
				Datasets: test.datasets, Watermarks: test.watermarks,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(units) != 1 {
				t.Fatalf("units=%+v want one canonical family", units)
			}
			family := units[0]
			if family.Dataset != canonicalWorkItemsDataset || family.WindowStart == nil ||
				!family.WindowStart.Equal(test.wantStart) {
				t.Fatalf("family=%+v want canonical start=%s", family, test.wantStart)
			}
			for _, dataset := range workitemcontract.FamilyDatasets() {
				flag := familyDatasetFlag(dataset)
				if !family.ProcessorFlags[flag] {
					t.Errorf("family flag %q missing from %+v", flag, family.ProcessorFlags)
				}
			}
		})
	}
}

func TestResolveInitialSyncDepthMatchesOverridePrecedenceAndFloor(t *testing.T) {
	dataset, integration, cap := 120, 60, 30
	if got := resolveInitialSyncDepth(&dataset, &integration, &cap); got != 30 {
		t.Fatalf("resolveInitialSyncDepth() = %d, want 30", got)
	}
	zero := 0
	if got := resolveInitialSyncDepth(&zero, nil, nil); got != 1 {
		t.Fatalf("resolveInitialSyncDepth(zero) = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// CHAOS-4054: capability is always on in the binary. BuildScheduledPlan now
// filters unconditionally on the execution registry
// (providersync.Descriptor -> RouteReady && Plannable); there is no
// plan-time route-switch gate, and no "unfiltered" mode, left.
// ---------------------------------------------------------------------------

func TestBuildScheduledPlanAlwaysPlansTheWorkItemFamilyCanonicalClaim(t *testing.T) {
	// The canonical work-items claim the family collapse emits is
	// RouteReady && Plannable unconditionally now -- there is no switch left
	// that could exclude it (successor to the deleted
	// TestBuildScheduledPlanExcludesRouteDisabledWorkItemFamilyCanonicalClaim /
	// TestBuildScheduledPlanPlansRouteEnabledWorkItemFamilyCanonicalClaim
	// pair, whose "switch off excludes it" half is now structurally
	// impossible).
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "work-items"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one canonical work-items claim", units)
	}
}

func TestBuildScheduledPlanNeverPlansAnAliasIdentityEvenWithACanonicalSibling(t *testing.T) {
	// Regression: the exact production failure shape from CHAOS-4047/4048.
	// github pr-comments aliases onto the canonical `prs` writer -- RouteReady
	// but never independently Plannable -- so it must never be minted as its
	// own unit, whether or not the sibling prs dataset is also requested.
	// Successor to
	// TestBuildScheduledPlanExcludesRouteDisabledAliasWithSiblingEnabled: the
	// exclusion no longer depends on any switch profile, it is unconditional.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "pr-comments"}, {Key: "prs"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].Dataset != "prs" {
		t.Fatalf(
			"units=%+v, want only the canonical prs writer: pr-comments is never independently plannable",
			units,
		)
	}
}

func TestBuildScheduledPlanPlansAKnownRoutablePairUnconditionally(t *testing.T) {
	// Input symmetry: a pair the matrix knows and marks RouteReady &&
	// Plannable is planned regardless of any input -- there is no
	// configuration knob left that could exclude it. Successor to the
	// TestBuildScheduledPlanExcludesRouteDisabledPairWithNoSibling /
	// TestBuildScheduledPlanPlansRouteEnabledPair /
	// TestBuildScheduledPlanPlansUnfilteredWhenRouteSwitchesIsNil trio: the
	// "switch off excludes it" and "nil switches plans unfiltered" framings
	// are both gone now that filtering is unconditional, so all three
	// collapse onto this one assertion.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "security"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one: security is RouteReady && Plannable unconditionally", units)
	}
}

func TestScheduledPlannerDescriptorFailsClosedForUnknownPair(t *testing.T) {
	// Input symmetry: a pair the checked-in matrix does not recognize at all
	// fails closed (ok=false), never an accidental route -- the fail-closed
	// guarantee BuildScheduledPlan's filtering relies on. Successor to
	// TestCompleteRouteSwitchesFailsClosedForUnknownPair, which called the
	// now-deleted CompleteRouteSwitches.Descriptor method; the package-level
	// providersync.Descriptor function is the sole entry point now.
	if _, ok := providersync.Descriptor("acme-corp", "widgets"); ok {
		t.Fatal("Descriptor() ok=true for an unrecognized pair, want fail-closed")
	}
}
