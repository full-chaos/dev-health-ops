package sync

import (
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// The work-item family is ratcheted like a HEAVY dataset for the providers
// whose route honours the window end: one bounded window a scheduled run,
// oldest first, the next window at the next run. These tests drive the whole
// planner, for every provider that has the family.

// workItemWindowRatchetedProviders is every provider that has the work-item
// family: all four routes honour the window end, so all four are ratcheted.
var workItemWindowRatchetedProviders = []string{"github", "gitlab", "jira", "linear"}

func TestWorkItemWindowProviderSetIsEveryFamilyProvider(t *testing.T) {
	got := slices.Clone(workItemWindowRatchetedProviders)
	slices.Sort(got)
	want := slices.Clone(workItemFamilyProviders)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ratcheted providers = %v, want every work-item family provider %v", got, want)
	}
	for _, provider := range workItemWindowRatchetedProviders {
		if _, bounded := workItemWindowBoundedProviders[provider]; !bounded {
			t.Errorf("%s is not in workItemWindowBoundedProviders", provider)
		}
	}
	if len(workItemWindowBoundedProviders) != len(workItemWindowRatchetedProviders) {
		t.Fatalf("workItemWindowBoundedProviders = %v, want exactly %v", workItemWindowBoundedProviders, workItemWindowRatchetedProviders)
	}
}

var workItemWindowNow = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)

func workItemFamilyPlanDatasets() []PlanDataset {
	keys := workitemcontract.FamilyDatasets()
	datasets := make([]PlanDataset, 0, len(keys))
	for _, key := range keys {
		datasets = append(datasets, PlanDataset{Key: key})
	}
	return datasets
}

func planWorkItemFamily(
	t *testing.T, provider, mode string, depthDays int, overlap time.Duration,
	watermarks map[WatermarkKey]time.Time,
) []PlannedUnit {
	t.Helper()
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: mode, Now: workItemWindowNow,
		IntegrationDepthDays: &depthDays, WatermarkOverlap: overlap,
		Sources:    []PlanSource{{ID: "source", ExternalID: "external", Provider: provider}},
		Datasets:   workItemFamilyPlanDatasets(),
		Watermarks: watermarks,
	})
	if err != nil {
		t.Fatalf("%s: BuildScheduledPlan: %v", provider, err)
	}
	return units
}

func onlyWorkItemsUnit(t *testing.T, provider string, units []PlannedUnit) PlannedUnit {
	t.Helper()
	if len(units) != 1 || units[0].Dataset != canonicalWorkItemsDataset {
		t.Fatalf("%s: units = %+v, want exactly one canonical work-items unit", provider, units)
	}
	if units[0].WindowStart == nil || units[0].WindowEnd == nil {
		t.Fatalf("%s: unit has an open window: %+v", provider, units[0])
	}
	return units[0]
}

// A source with no watermark at all -- a first sync, and equally a source
// whose every unit failed so no watermark was ever stamped -- plans the FIRST
// window of the depth, not the whole depth.
func TestWorkItemFamilyColdStartPlansTheFirstWindowForEveryProvider(t *testing.T) {
	for _, provider := range workItemWindowRatchetedProviders {
		t.Run(provider, func(t *testing.T) {
			unit := onlyWorkItemsUnit(t, provider,
				planWorkItemFamily(t, provider, SyncModeIncremental, 90, 0, nil))
			wantStart := workItemWindowNow.AddDate(0, 0, -90)
			wantEnd := wantStart.Add(defaultIncrementalHeavyMaxWindowDays * 24 * time.Hour)
			if !unit.WindowStart.Equal(wantStart) || !unit.WindowEnd.Equal(wantEnd) {
				t.Fatalf("window = [%s, %s], want [%s, %s]", unit.WindowStart, unit.WindowEnd, wantStart, wantEnd)
			}
			// The window rule widens; the unit's cost class does not. It
			// selects the request budget and the admission cap, and claim
			// validation requires the registry's value.
			if unit.CostClass != "medium" {
				t.Fatalf("cost_class = %q, want the registry's medium", unit.CostClass)
			}
		})
	}
}

// Stamping every family watermark at the window end, as a successful unit
// does, and planning again walks the whole depth: thirteen contiguous windows
// for ninety days, the last one ending at now, and then no unit.
func TestWorkItemFamilyWindowsWalkTheWholeDepthWithoutAGap(t *testing.T) {
	for _, provider := range workItemWindowRatchetedProviders {
		t.Run(provider, func(t *testing.T) {
			watermarks := map[WatermarkKey]time.Time{}
			depthStart := workItemWindowNow.AddDate(0, 0, -90)
			previousEnd := depthStart
			windows := 0
			for {
				units := planWorkItemFamily(t, provider, SyncModeIncremental, 90, 0, watermarks)
				if len(units) == 0 {
					break
				}
				unit := onlyWorkItemsUnit(t, provider, units)
				windows++
				if windows > 20 {
					t.Fatalf("no convergence after %d windows; last = [%s, %s]", windows, unit.WindowStart, unit.WindowEnd)
				}
				if !unit.WindowStart.Equal(previousEnd) {
					t.Fatalf("window %d starts at %s, want the previous end %s", windows, unit.WindowStart, previousEnd)
				}
				if span := unit.WindowEnd.Sub(*unit.WindowStart); span <= 0 || span > defaultIncrementalHeavyMaxWindowDays*24*time.Hour {
					t.Fatalf("window %d spans %s, want a positive span of at most the cap", windows, span)
				}
				previousEnd = *unit.WindowEnd
				for _, dataset := range workitemcontract.FamilyDatasets() {
					watermarks[WatermarkKey{SourceID: "external", Dataset: dataset}] = previousEnd
				}
			}
			if windows != 13 {
				t.Fatalf("windows = %d, want 13 for a 90-day depth at 7 days a window", windows)
			}
			if !previousEnd.Equal(workItemWindowNow) {
				t.Fatalf("last window ends at %s, want now %s", previousEnd, workItemWindowNow)
			}
		})
	}
}

// A source that is already caught up keeps its natural window: the cap is a
// minimum, never an assignment, so the hourly run is unchanged.
func TestWorkItemFamilyWatermarkInsideTheCapKeepsItsNaturalEnd(t *testing.T) {
	for _, provider := range workItemWindowRatchetedProviders {
		t.Run(provider, func(t *testing.T) {
			stored := workItemWindowNow.Add(-time.Hour)
			watermarks := map[WatermarkKey]time.Time{}
			for _, dataset := range workitemcontract.FamilyDatasets() {
				watermarks[WatermarkKey{SourceID: "external", Dataset: dataset}] = stored
			}
			unit := onlyWorkItemsUnit(t, provider,
				planWorkItemFamily(t, provider, SyncModeIncremental, 90, 0, watermarks))
			if !unit.WindowStart.Equal(stored) || !unit.WindowEnd.Equal(workItemWindowNow) {
				t.Fatalf("window = [%s, %s], want [%s, %s]", unit.WindowStart, unit.WindowEnd, stored, workItemWindowNow)
			}
		})
	}
}

// The members own separate watermarks. When they differ, each member's own
// capped window ends at a different instant and the merged span would be wider
// than one window; the unit is capped from its earliest start.
func TestWorkItemFamilyMergedWindowIsCappedFromTheEarliestMemberStart(t *testing.T) {
	earliest := workItemWindowNow.AddDate(0, 0, -60)
	later := workItemWindowNow.AddDate(0, 0, -20)
	watermarks := map[WatermarkKey]time.Time{}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		watermarks[WatermarkKey{SourceID: "external", Dataset: dataset}] = later
	}
	watermarks[WatermarkKey{SourceID: "external", Dataset: "work-item-history"}] = earliest

	unit := onlyWorkItemsUnit(t, "github",
		planWorkItemFamily(t, "github", SyncModeIncremental, 90, 0, watermarks))
	wantEnd := earliest.Add(defaultIncrementalHeavyMaxWindowDays * 24 * time.Hour)
	if !unit.WindowStart.Equal(earliest) || !unit.WindowEnd.Equal(wantEnd) {
		t.Fatalf("window = [%s, %s], want [%s, %s]: the merged span of members "+
			"capped one by one is 47 days here", unit.WindowStart, unit.WindowEnd, earliest, wantEnd)
	}
}

// With a watermark overlap the next window starts the overlap before the
// stored watermark and still ends strictly after it, so a successful window
// always advances.
func TestWorkItemFamilyWindowAdvancesPastTheWatermarkUnderAnOverlap(t *testing.T) {
	overlap := 36 * time.Hour
	stored := workItemWindowNow.AddDate(0, 0, -40)
	watermarks := map[WatermarkKey]time.Time{}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		watermarks[WatermarkKey{SourceID: "external", Dataset: dataset}] = stored
	}
	unit := onlyWorkItemsUnit(t, "github",
		planWorkItemFamily(t, "github", SyncModeIncremental, 90, overlap, watermarks))
	wantStart := stored.Add(-overlap)
	wantEnd := wantStart.Add(defaultIncrementalHeavyMaxWindowDays * 24 * time.Hour)
	if !unit.WindowStart.Equal(wantStart) || !unit.WindowEnd.Equal(wantEnd) {
		t.Fatalf("window = [%s, %s], want [%s, %s]", unit.WindowStart, unit.WindowEnd, wantStart, wantEnd)
	}
	if !unit.WindowEnd.After(stored) {
		t.Fatalf("window end %s does not pass the stored watermark %s", unit.WindowEnd, stored)
	}
}

// The window length is the HEAVY rule's own knob; no second setting exists.
func TestWorkItemFamilyWindowHonoursTheHeavyWindowSetting(t *testing.T) {
	t.Setenv(incrementalHeavyMaxWindowDaysEnv, "3")
	unit := onlyWorkItemsUnit(t, "linear",
		planWorkItemFamily(t, "linear", SyncModeIncremental, 90, 0, nil))
	if span := unit.WindowEnd.Sub(*unit.WindowStart); span != 3*24*time.Hour {
		t.Fatalf("window spans %s, want 72h", span)
	}
}

// A full resync is outside the ratchet for every dataset (the rule is
// incremental only); the family follows that, and plans its whole depth.
func TestWorkItemFamilyFullResyncIsNotRatcheted(t *testing.T) {
	unit := onlyWorkItemsUnit(t, "github",
		planWorkItemFamily(t, "github", SyncModeFullResync, 90, 0, nil))
	wantStart := workItemWindowNow.AddDate(0, 0, -90)
	if !unit.WindowStart.Equal(wantStart) || !unit.WindowEnd.Equal(workItemWindowNow) {
		t.Fatalf("window = [%s, %s], want the whole depth [%s, %s]", unit.WindowStart, unit.WindowEnd, wantStart, workItemWindowNow)
	}
}

// WindowRatchetClass names the ratchet scope: every family dataset of a
// window-bounded provider, whatever its registry class, and nothing else.
func TestWindowRatchetClassCoversExactlyTheBoundedFamilyAndHeavyDatasets(t *testing.T) {
	for provider, datasets := range supportedProviderDatasets {
		_, bounded := workItemWindowBoundedProviders[provider]
		for dataset := range datasets {
			spec, ok := datasetSpecification(provider, dataset)
			if !ok {
				t.Fatalf("no specification for %s/%s", provider, dataset)
			}
			want := spec.CostClass
			if bounded && workitemcontract.IsFamilyDataset(dataset) {
				want = heavyCostClass
			}
			if got := WindowRatchetClass(provider, dataset, spec.CostClass); got != want {
				t.Errorf("WindowRatchetClass(%q, %q, %q) = %q, want %q", provider, dataset, spec.CostClass, got, want)
			}
		}
	}
	// The provider key is matched without regard to case, as the planner
	// lower-cases a source's provider everywhere else.
	if got := WindowRatchetClass("GitHub", "work-item-labels", "light"); got != heavyCostClass {
		t.Errorf("WindowRatchetClass(GitHub, work-item-labels, light) = %q, want %q", got, heavyCostClass)
	}
}

// The admin watermark view reports "catching up" from the same scope, so a
// family dataset that trails by more than one window is shown as catching up
// with its remaining runs, and one inside the window is not.
func TestWorkItemFamilyCatchUpIsReportedFromTheRatchetScope(t *testing.T) {
	capDays, netAdvance := HeavyRatchet()
	behind := workItemWindowNow.AddDate(0, 0, -90)
	lag := ComputeWatermarkLag(WindowRatchetClass("github", "work-items", "medium"), &behind, workItemWindowNow, capDays, netAdvance)
	if !lag.CatchingUp || lag.TicksBehind == nil || *lag.TicksBehind != 13 {
		t.Fatalf("lag = %+v, want catching up with 13 runs behind", lag)
	}
	inside := workItemWindowNow.Add(-time.Hour)
	lag = ComputeWatermarkLag(WindowRatchetClass("github", "work-items", "medium"), &inside, workItemWindowNow, capDays, netAdvance)
	if lag.CatchingUp || lag.TicksBehind != nil {
		t.Fatalf("lag = %+v, want not catching up", lag)
	}
	lag = ComputeWatermarkLag(WindowRatchetClass("github", "commits", "medium"), &behind, workItemWindowNow, capDays, netAdvance)
	if lag.CatchingUp {
		t.Fatalf("lag = %+v: a medium dataset outside the family is not ratcheted", lag)
	}
	for _, provider := range []string{"gitlab", "jira", "linear"} {
		lag = ComputeWatermarkLag(WindowRatchetClass(provider, "work-items", "medium"), &behind, workItemWindowNow, capDays, netAdvance)
		if !lag.CatchingUp {
			t.Fatalf("%s lag = %+v: the family of every provider is caught up window by window", provider, lag)
		}
	}
}

// A work-items dataset whose own query the window does not bound (Jira fetch_all, a JQL override) runs that
// full query in every unit; it is planned as ONE window for the whole family, and the decision is counted.
func TestWorkItemFamilyOfAFullQueryDatasetIsOneWindow(t *testing.T) {
	plan := func(fullQuery bool) PlannedUnit {
		datasets := workItemFamilyPlanDatasets()
		for index := range datasets {
			if datasets[index].Key == canonicalWorkItemsDataset {
				datasets[index].FullQuery = fullQuery
			}
		}
		depth := 90
		units, err := BuildScheduledPlan(PlannerInput{
			OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: workItemWindowNow,
			IntegrationDepthDays: &depth,
			Sources:              []PlanSource{{ID: "source", ExternalID: "external", Provider: "jira"}},
			Datasets:             datasets,
		})
		if err != nil {
			t.Fatal(err)
		}
		return onlyWorkItemsUnit(t, "jira", units)
	}
	before := globalPlanGateTelemetry.snapshotForTest("jira", canonicalWorkItemsDataset, planGateOutcomeFullQueryWindow)
	full := plan(true)
	if want := workItemWindowNow.AddDate(0, 0, -90); !full.WindowStart.Equal(want) || !full.WindowEnd.Equal(workItemWindowNow) {
		t.Fatalf("full-query window = [%s, %s], want the whole depth [%s, %s]", full.WindowStart, full.WindowEnd, want, workItemWindowNow)
	}
	if got := globalPlanGateTelemetry.snapshotForTest("jira", canonicalWorkItemsDataset, planGateOutcomeFullQueryWindow); got != before+1 {
		t.Fatalf("full_query_window outcome moved %d -> %d, want +1", before, got)
	}
	bounded := plan(false)
	if span := bounded.WindowEnd.Sub(*bounded.WindowStart); span != 7*24*time.Hour {
		t.Fatalf("bounded window span = %s, want 7 days", span)
	}
}
