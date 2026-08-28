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
	// CHAOS-4078: "prs" is now ALSO a fold-family member (the PR-social
	// family's own canonical identity), so it no longer plans through the
	// individual per-dataset path -- it plans via foldContributingFamilyUnit,
	// alongside the work-item family's canonical claim. Two units, found by
	// dataset key rather than a fixed index since the fold call order is an
	// implementation detail.
	if len(units) != 2 {
		t.Fatalf("len(units) = %d, want 2: %+v", len(units), units)
	}
	var family, prsUnit *PlannedUnit
	for index := range units {
		switch units[index].Dataset {
		case canonicalWorkItemsDataset:
			family = &units[index]
		case "prs":
			prsUnit = &units[index]
		}
	}
	if family == nil {
		t.Fatalf("no canonical work-items unit in %+v", units)
	}
	if family.WindowStart == nil || !family.WindowStart.Equal(items) {
		t.Fatalf("family unit = %+v, want canonical work-items at earliest watermark", *family)
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
	if prsUnit == nil {
		t.Fatalf("no prs unit (PR-social fold) in %+v", units)
	}
	if !prsUnit.ProcessorFlags["family_dataset_prs"] || !prsUnit.ProcessorFlags["sync_prs"] {
		t.Errorf("prs fold unit missing expected flags: %+v", prsUnit.ProcessorFlags)
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

func TestBuildScheduledPlanFoldsAnAliasOnlySelectionOntoItsCanonicalWriter(t *testing.T) {
	// CHAOS-4078 acceptance / CHAOS-4125 root cause: an org that enables ONLY
	// an alias -- no canonical sibling at all -- previously planned NOTHING
	// (TestBuildScheduledPlanNeverPlansAnAliasIdentityEvenWithACanonicalSibling
	// only covers the sibling-present case). Now it plans exactly one unit
	// under the canonical writer, carrying the alias's own completion flag.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name          string
		provider      string
		alias         string
		canonical     string
		wantCostClass string
	}{
		{"github pr-comments", "github", "pr-comments", "prs", "medium"},
		{"github pr-reviews", "github", "pr-reviews", "prs", "medium"},
		// tests is HEAVY while cicd (its canonical) is MEDIUM: the fold must
		// stamp the heavier class so a tests-only unit still enters
		// dispatch/provider-budget buckets as heavy (CHAOS-4078 review finding).
		{"github tests", "github", "tests", "cicd", "heavy"},
		{"gitlab pr-comments", "gitlab", "pr-comments", "prs", "medium"},
		{"gitlab tests", "gitlab", "tests", "cicd", "heavy"},
	} {
		t.Run(test.name, func(t *testing.T) {
			units, err := BuildScheduledPlan(PlannerInput{
				OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
				Sources: []PlanSource{
					{ID: "source", ExternalID: "owner/repo", Provider: test.provider, FullName: "owner/repo"},
				},
				Datasets: []PlanDataset{{Key: test.alias}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(units) != 1 || units[0].Dataset != test.canonical {
				t.Fatalf(
					"units=%+v, want exactly one unit under canonical %q for alias-only selection %q",
					units, test.canonical, test.alias,
				)
			}
			flag := familyDatasetFlag(test.alias)
			if !units[0].ProcessorFlags[flag] {
				t.Errorf("completion flag %q missing from %+v", flag, units[0].ProcessorFlags)
			}
			if units[0].CostClass != test.wantCostClass {
				t.Errorf("cost_class=%q, want %q for %+v", units[0].CostClass, test.wantCostClass, units[0])
			}
		})
	}
}

func TestBuildScheduledPlanTestOpsFoldStaysMediumWhenOnlyCicdIsEnabled(t *testing.T) {
	// Inverse of the heavy-cost-class regression: a plain cicd-only selection
	// (no tests alias) must NOT be inflated to heavy -- the weighted max only
	// raises the class, never lowers a genuinely lighter one.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "cicd"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].Dataset != "cicd" || units[0].CostClass != "medium" {
		t.Fatalf("units=%+v, want exactly one medium cicd unit", units)
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

func TestBuildScheduledPlanRequiresExecutedProofOrWaiver(t *testing.T) {
	// CHAOS-4060: fixture/golden proof (RouteReady && Plannable) stops being
	// sufficient. github/security carries no ExecutedProofWaiver, so it must
	// license new work only when the CHAOS-4060 evidence snapshot actually
	// proves a live executed run for it, OR when the pair has never been
	// attempted at all (bootstrap convergence -- see
	// providersync.ExecutedProofEvidence). It must NOT plan when a non-nil
	// snapshot records the pair as ATTEMPTED but never proven, which is the
	// negative control: the gate must actually fail when proof is absent
	// despite a real attempt, not merely pass when proof happens to be
	// present or the pair is untouched.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	input := func(evidence *providersync.ExecutedProofEvidence) PlannerInput {
		return PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
			Sources:       []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
			Datasets:      []PlanDataset{{Key: "security"}},
			ExecutedProof: evidence,
		}
	}

	// Negative control: github/security has been attempted (a real prior
	// unit exists for it) but never proven. Other pairs being proven is
	// irrelevant -- this pair's own state is what the gate reads.
	units, err := BuildScheduledPlan(input(&providersync.ExecutedProofEvidence{
		Proven:    map[string]bool{"github/commits": true},
		Attempted: map[string]bool{"github/commits": true, "github/security": true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 0 {
		t.Fatalf("units=%+v, want zero: github/security was attempted and never proven, and has no waiver", units)
	}

	// Positive control: proof exists for this exact pair.
	units, err = BuildScheduledPlan(input(&providersync.ExecutedProofEvidence{
		Proven:    map[string]bool{"github/security": true},
		Attempted: map[string]bool{"github/security": true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one: github/security has live executed proof", units)
	}

	// Empty-but-non-nil evidence: the gate is wired and enforced, but
	// github/security has never been ATTEMPTED at all -- this is the
	// CHAOS-4060 bootstrap case (a fresh database, or any pair with zero
	// sync_run_units history), and it must plan so the pair can earn its
	// own first attempt.
	units, err = BuildScheduledPlan(input(&providersync.ExecutedProofEvidence{
		Proven: map[string]bool{}, Attempted: map[string]bool{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf(
			"units=%+v, want exactly one: never-attempted must bootstrap through, not deadlock",
			units,
		)
	}

	// Nil evidence: this caller has not wired the gate -- pre-CHAOS-4060
	// behavior, unchanged. This is exactly
	// TestBuildScheduledPlanPlansAKnownRoutablePairUnconditionally's
	// assertion, repeated here to pin that "not wired" and "wired but
	// never attempted" reach the same outcome by different reasoning.
	units, err = BuildScheduledPlan(input(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units=%+v, want exactly one: nil evidence means the gate is not wired by this caller", units)
	}
}

func TestBuildScheduledPlanExecutedProofWaiverBypassesMissingEvidence(t *testing.T) {
	// github/repo-metadata carries CHAOS-4054's ratified interim
	// ExecutedProofWaiver (no production users yet). It must keep planning
	// even against a non-nil, fully-enforced evidence snapshot that proves
	// nothing for it -- the waiver is the "explicit, dated operator waiver"
	// alternative the ticket names, not merely an absence of enforcement.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "repo-metadata"}},
		ExecutedProof: &providersync.ExecutedProofEvidence{
			Proven: map[string]bool{}, Attempted: map[string]bool{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf(
			"units=%+v, want exactly one: github/repo-metadata's ExecutedProofWaiver bypasses empty evidence",
			units,
		)
	}
}

func TestBuildScheduledPlanRequiresExecutedProofForWorkItemFamilyCanonicalClaim(t *testing.T) {
	// Same CHAOS-4060 requirement as TestBuildScheduledPlanRequiresExecutedProofOrWaiver,
	// applied to the family-collapse gate: the canonical work-items claim
	// must not plan against non-nil evidence recording it as attempted but
	// never proven.
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "work-items"}},
		ExecutedProof: &providersync.ExecutedProofEvidence{
			Proven:    map[string]bool{"github/commits": true},
			Attempted: map[string]bool{"github/commits": true, "github/work-items": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 0 {
		t.Fatalf("units=%+v, want zero: github/work-items was attempted and never proven, and has no waiver", units)
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
