package sync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

var workItemFamilyDatasets = []PlanDataset{
	{Key: "work-items"}, {Key: "work-item-labels"}, {Key: "work-item-projects"},
	{Key: "work-item-history"}, {Key: "work-item-comments"},
}

func backfillUnitsFor(t *testing.T, provider string, datasets []PlanDataset, since, before time.Time) []PlannedUnit {
	t.Helper()
	units, err := BuildBackfillPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeBackfill,
		Now:   time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC),
		Since: &since, Before: &before,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: provider, FullName: "owner/repo"}},
		Datasets: datasets,
	})
	if err != nil {
		t.Fatal(err)
	}
	return units
}

// A request whose exclusive end is exactly UTC midnight must not leave a
// last chunk that starts at that midnight: its window would be [D, D), a unit
// the work-items route refuses as malformed on every attempt.
func TestBuildBackfillPlanNeverPlansAZeroWidthWindow(t *testing.T) {
	t.Parallel()
	utc := func(month time.Month, day, hour int) time.Time {
		return time.Date(2026, month, day, hour, 0, 0, 0, time.UTC)
	}
	for _, test := range []struct {
		name      string
		provider  string
		datasets  []PlanDataset
		since     time.Time
		before    time.Time
		wantUnits int
	}{
		{name: "github family, production shape", provider: "github", datasets: workItemFamilyDatasets,
			since: utc(8, 20, 5), before: utc(10, 8, 0), wantUnits: 7},
		{name: "github standalone dataset", provider: "github", datasets: []PlanDataset{{Key: "security"}},
			since: utc(8, 20, 5), before: utc(10, 8, 0), wantUnits: 7},
		{name: "linear family, 14-day chunk", provider: "linear", datasets: workItemFamilyDatasets,
			since: utc(9, 1, 0), before: utc(9, 15, 0), wantUnits: 1},
		{name: "one day exactly", provider: "github", datasets: workItemFamilyDatasets,
			since: utc(10, 8, 0), before: utc(10, 9, 0), wantUnits: 1},
		{name: "end one hour past midnight keeps its day", provider: "github", datasets: workItemFamilyDatasets,
			since: utc(10, 1, 0), before: utc(10, 8, 1), wantUnits: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			units := backfillUnitsFor(t, test.provider, test.datasets, test.since, test.before)
			if len(units) != test.wantUnits {
				t.Fatalf("len(units) = %d, want %d: %+v", len(units), test.wantUnits, units)
			}
			for _, unit := range units {
				if unit.WindowStart == nil || unit.WindowEnd == nil || !unit.WindowStart.Before(*unit.WindowEnd) {
					t.Fatalf("unit window is not a non-empty half-open range: %+v", unit)
				}
				if unit.Dataset == canonicalWorkItemsDataset && test.provider == "github" {
					if _, err := providersync.WorkItemsUnitWindowDays(unit.WindowStart, unit.WindowEnd, test.before); err != nil {
						t.Fatalf("a planned work-items window the route refuses: %v (%s .. %s)", err, unit.WindowStart, unit.WindowEnd)
					}
				}
			}
			last := units[len(units)-1]
			if !last.WindowEnd.Equal(test.before) {
				t.Fatalf("last window ends %s, want the requested end %s", last.WindowEnd, test.before)
			}
		})
	}
}

// The planned windows tile the requested range: each end is the next start,
// the first start is the requested start and the last end the requested end,
// so no instant of [since, before) is left out by a route that treats the end
// as exclusive.
func TestBuildBackfillPlanWindowsTileTheRequestedRange(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 8, 20, 5, 0, 0, 0, time.UTC)
	before := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		provider string
		datasets []PlanDataset
	}{
		{"github family", "github", workItemFamilyDatasets},
		{"github standalone", "github", []PlanDataset{{Key: "security"}}},
		{"linear family", "linear", workItemFamilyDatasets},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			units := backfillUnitsFor(t, test.provider, test.datasets, since, before)
			if len(units) < 2 {
				t.Fatalf("len(units) = %d, want several windows to tile", len(units))
			}
			if !units[0].WindowStart.Equal(since) {
				t.Errorf("first window starts %s, want the requested start %s", units[0].WindowStart, since)
			}
			for index := 0; index+1 < len(units); index++ {
				if !units[index].WindowEnd.Equal(*units[index+1].WindowStart) {
					t.Errorf("window %d ends %s but window %d starts %s: gap or overlap",
						index, units[index].WindowEnd, index+1, units[index+1].WindowStart)
				}
			}
			if last := units[len(units)-1]; !last.WindowEnd.Equal(before) {
				t.Errorf("last window ends %s, want the requested end %s", last.WindowEnd, before)
			}
		})
	}
}
