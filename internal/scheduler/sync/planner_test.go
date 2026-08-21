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
// CHAOS-4047: the plan-time route-switch gate. Owner decision, 2026-08-21:
// the Celery fallback is retired, so this gates on the switch alone -- no
// consumer-presence signal is consulted, symmetric with the Python planner.
// ---------------------------------------------------------------------------

func TestBuildScheduledPlanExcludesRouteDisabledWorkItemFamilyCanonicalClaim(t *testing.T) {
	// Codex review finding: family ALIASES are deliberately unchecked (their
	// admission is the atomic-family collapse's business), but the CANONICAL
	// claim the collapse emits ("work-items") is an ordinary routable pair
	// with its own switch (WORKER_GITHUB_WORK_ITEMS_ENABLED) that must still
	// gate it -- skipping the whole family branch must not also skip that.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	switches := providersync.CompleteRouteSwitches{GithubWorkItems: false}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:      []PlanDataset{{Key: "work-items"}},
		RouteSwitches: &switches,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 0 {
		t.Fatalf("units=%+v, want none: the canonical work-items claim's own switch is off", units)
	}
}

func TestBuildScheduledPlanPlansRouteEnabledWorkItemFamilyCanonicalClaim(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	switches := providersync.CompleteRouteSwitches{GithubWorkItems: true}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:      []PlanDataset{{Key: "work-items"}},
		RouteSwitches: &switches,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one canonical work-items claim", units)
	}
}

func TestBuildScheduledPlanExcludesRouteDisabledAliasWithSiblingEnabled(t *testing.T) {
	// Regression: the exact production failure shape from CHAOS-4047/4048.
	// github pr-comments is a mutually exclusive alias of the prs writer
	// (config.go:337 rejects enabling both). With prs enabled and
	// pr-comments left off -- the actual prod switch profile -- a pre-fix
	// planner still minted a pr-comments unit that terminalized instantly as
	// feature_disabled (200 such units in one window).
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	switches := providersync.CompleteRouteSwitches{GithubPRs: true, GithubPRComments: false}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:      []PlanDataset{{Key: "pr-comments"}},
		RouteSwitches: &switches,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 0 {
		t.Fatalf("units=%+v, want none: pr-comments is a disabled alias of the enabled prs writer", units)
	}
}

func TestBuildScheduledPlanExcludesRouteDisabledPairWithNoSibling(t *testing.T) {
	// Input symmetry: exclusion is a general "switch is off" rule. github
	// security has no alias sibling at all -- proving the gate is not
	// secretly alias-specific.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	switches := providersync.CompleteRouteSwitches{GithubSecurity: false}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:      []PlanDataset{{Key: "security"}},
		RouteSwitches: &switches,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 0 {
		t.Fatalf("units=%+v, want none: security's own switch is off, no alias sibling involved", units)
	}
}

func TestBuildScheduledPlanPlansRouteEnabledPair(t *testing.T) {
	// Input symmetry: the gate excludes only disabled pairs, not everything.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	switches := providersync.CompleteRouteSwitches{GithubSecurity: true}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:      []PlanDataset{{Key: "security"}},
		RouteSwitches: &switches,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one enabled pair", units)
	}
}

func TestBuildScheduledPlanPlansUnfilteredWhenRouteSwitchesIsNil(t *testing.T) {
	// Input symmetry: nil RouteSwitches (sync.provider_unit not a River
	// outbox route) plans unfiltered, exactly like before CHAOS-4047.
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
		t.Fatalf("units=%+v, want exactly one: nil RouteSwitches must plan unfiltered", units)
	}
}

func TestCompleteRouteSwitchesFailsClosedForUnknownPair(t *testing.T) {
	// Input symmetry: a pair the checked-in matrix does not recognize at all
	// fails closed (ok=false), never an accidental route -- the fail-closed
	// guarantee BuildScheduledPlan's exclusion check relies on.
	switches := providersync.CompleteRouteSwitches{}
	if _, ok := switches.Descriptor("acme-corp", "widgets"); ok {
		t.Fatal("Descriptor() ok=true for an unrecognized pair, want fail-closed")
	}
}
