package sync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func testOpsPlanSpan(t *testing.T, mode string, now, watermark time.Time, blocked map[string]int, keys ...string) time.Duration {
	t.Helper()
	datasets := make([]PlanDataset, 0, len(keys))
	watermarks := map[WatermarkKey]time.Time{}
	for _, key := range keys {
		datasets = append(datasets, PlanDataset{Key: key})
		watermarks[WatermarkKey{SourceID: "owner/repo", Dataset: key}] = watermark
	}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: mode, Now: now,
		Sources:              []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:             datasets,
		Watermarks:           watermarks,
		TestOpsBudgetBlocked: blocked,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].Dataset != "cicd" || units[0].WindowStart == nil || units[0].WindowEnd == nil {
		t.Fatalf("units = %+v, want one cicd unit with a bounded window", units)
	}
	return units[0].WindowEnd.Sub(*units[0].WindowStart)
}

// CHAOS-13827: cicd is MEDIUM and uncapped, so before this change the folded cicd+tests unit of a source 10 days behind
// spanned all 10 days, past the 7-day HEAVY cap tests alone gets. The merged span now takes the HEAVY cap when tests
// contributes. A cicd-only selection keeps its MEDIUM, uncapped window.
func TestBuildScheduledPlanCapsTheFoldedTestOpsWindowWhenTestsContributes(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * 24 * time.Hour)
	for _, test := range []struct {
		name string
		keys []string
		want time.Duration
	}{
		{"cicd and tests", []string{"cicd", "tests"}, 7 * 24 * time.Hour},
		{"tests only", []string{"tests"}, 7 * 24 * time.Hour},
		{"cicd only", []string{"cicd"}, 10 * 24 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := testOpsPlanSpan(t, SyncModeIncremental, now, stale, nil, test.keys...); got != test.want {
				t.Fatalf("span = %s, want %s", got, test.want)
			}
		})
	}
}

// Each run-inventory budget block among the source's newest units halves the folded window, down to one hour. No count
// plans exactly as before; a full resync is never narrowed; another source's count does not apply.
func TestBuildScheduledPlanHalvesTheTestOpsWindowPerBudgetBlockedUnit(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	behind := now.Add(-72 * time.Hour)
	for _, test := range []struct {
		name    string
		mode    string
		blocked map[string]int
		want    time.Duration
	}{
		{"no count", SyncModeIncremental, nil, 72 * time.Hour},
		{"one block", SyncModeIncremental, map[string]int{"source": 1}, 36 * time.Hour},
		{"three blocks", SyncModeIncremental, map[string]int{"source": 3}, 9 * time.Hour},
		{"the floor", SyncModeIncremental, map[string]int{"source": 12}, providersync.TestOpsWindowHalvingFloor},
		{"another source's count", SyncModeIncremental, map[string]int{"other": 3}, 72 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := testOpsPlanSpan(t, test.mode, now, behind, test.blocked, "cicd", "tests"); got != test.want {
				t.Fatalf("span = %s, want %s", got, test.want)
			}
		})
	}
	units, err := BuildScheduledPlan(PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeFullResync, Now: now,
		Sources:              []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets:             []PlanDataset{{Key: "cicd"}, {Key: "tests"}},
		TestOpsBudgetBlocked: map[string]int{"source": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || units[0].WindowEnd == nil || !units[0].WindowEnd.Equal(now) {
		t.Fatalf("full resync units = %+v, want one unit that ends at now (not narrowed)", units)
	}
}

// The state the change exists to reach: a source too dense for one unit's run budget catches up. Hourly units over a
// 10-day gap; a unit's runs phase lists its window plus the 6 h lookback at `density` runs an hour and is blocked (the
// watermark held) when that passes the 10,000-run budget, else it stamps both watermarks at its end. With the blocked
// count fed back the gap closes; with the count dropped (the planner before this change) every unit is blocked.
// 365 runs an hour is the busiest prod repository's peak day (8,762 runs, 2026-11); 1,000 an hour is about three times it.
func TestTestOpsWindowNarrowingDrainsADenseSourceAfterAnOutage(t *testing.T) {
	const budget = 10000.0
	lookback := 6 * time.Hour
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	simulate := func(density float64, feedBack bool, hours int) (caughtUpAt int, blockedUnits int) {
		watermark := start.Add(-10 * 24 * time.Hour)
		var history []bool
		for hour := 1; hour <= hours; hour++ {
			now := start.Add(time.Duration(hour) * time.Hour)
			var blocked map[string]int
			if feedBack {
				if count := providersync.TestOpsBudgetBlockedCount(history); count > 0 {
					blocked = map[string]int{"source": count}
				}
			}
			span := testOpsPlanSpan(t, SyncModeIncremental, now, watermark, blocked, "cicd", "tests")
			unitBlocked := (span+lookback).Hours()*density > budget
			if unitBlocked {
				blockedUnits++
			} else {
				watermark = watermark.Add(span)
			}
			history = append([]bool{unitBlocked}, history...)
			if caughtUpAt == 0 && now.Sub(watermark) <= 2*time.Hour {
				caughtUpAt = hour
			}
		}
		return caughtUpAt, blockedUnits
	}
	for _, test := range []struct {
		density  float64
		hours    int
		deadline int
	}{
		{365, 96, 48},
		{1000, 480, 400},
	} {
		caughtUp, blocked := simulate(test.density, true, test.hours)
		t.Logf("density %.0f/h, count fed back: caught up (lag <= 2h) at unit %d, %d of %d units blocked", test.density, caughtUp, blocked, test.hours)
		if caughtUp == 0 || caughtUp > test.deadline {
			t.Errorf("density %.0f/h: caught up at unit %d, want by unit %d", test.density, caughtUp, test.deadline)
		}
		caughtUp, blocked = simulate(test.density, false, test.hours)
		t.Logf("density %.0f/h, count dropped: caught up at unit %d, %d of %d units blocked", test.density, caughtUp, blocked, test.hours)
		if caughtUp != 0 || blocked != test.hours {
			t.Errorf("density %.0f/h without the count: caught up at %d with %d of %d blocked, want never and all blocked", test.density, caughtUp, blocked, test.hours)
		}
	}
}
