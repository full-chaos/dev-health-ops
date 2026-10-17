package sync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-8802: after a size or time-limit failure the next work-items window is
// halved; after 3 equal failures the source is planned once a day.

func backoffInput(now time.Time, watermark time.Time, streak providersync.FailureStreak) PlannerInput {
	return PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: SyncModeIncremental, Now: now,
		Sources:  []PlanSource{{ID: "source", ExternalID: "owner/repo", Provider: "github", FullName: "owner/repo"}},
		Datasets: []PlanDataset{{Key: "work-items"}},
		Watermarks: map[WatermarkKey]time.Time{
			{SourceID: "owner/repo", Dataset: "work-items"}: watermark,
		},
		FailureStreaks: map[string]providersync.FailureStreak{"source": streak},
	}
}

func workItemsUnit(t *testing.T, input PlannerInput) *PlannedUnit {
	t.Helper()
	units, err := BuildScheduledPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	for index := range units {
		if units[index].Dataset == canonicalWorkItemsDataset {
			return &units[index]
		}
	}
	return nil
}

func TestWorkItemsWindowHalvesAfterSizeOrTimeLimitFailures(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	watermark := now.Add(-80 * 24 * time.Hour)
	naturalSpan := func() time.Duration {
		unit := workItemsUnit(t, backoffInput(now, watermark, providersync.FailureStreak{}))
		if unit == nil || unit.WindowStart == nil || unit.WindowEnd == nil {
			t.Fatalf("no baseline unit: %+v", unit)
		}
		return unit.WindowEnd.Sub(*unit.WindowStart)
	}()

	for _, tc := range []struct {
		class string
		count int
		want  time.Duration
	}{
		{"result_too_large", 1, naturalSpan / 2},
		{"result_too_large", 2, naturalSpan / 4},
		{"time_limit", 1, naturalSpan / 2},
		{"time_limit", 8, providersync.WindowHalvingFloor},
	} {
		streak := providersync.FailureStreak{Count: tc.count, ShrinkCount: tc.count, Class: tc.class, LastAt: now.Add(-time.Hour)}
		if tc.count >= providersync.FailureStreakThreshold {
			streak.LastAt = now.Add(-48 * time.Hour)
		}
		unit := workItemsUnit(t, backoffInput(now, watermark, streak))
		if unit == nil {
			t.Fatalf("%s x%d: no unit planned", tc.class, tc.count)
		}
		if got := unit.WindowEnd.Sub(*unit.WindowStart); got != tc.want {
			t.Errorf("%s x%d: window = %s, want %s", tc.class, tc.count, got, tc.want)
		}
		if !unit.WindowStart.Equal(watermark) {
			t.Errorf("%s x%d: window start moved to %s, want the watermark %s", tc.class, tc.count, unit.WindowStart, watermark)
		}
	}
}

func TestWorkItemsWindowHalvesOncePerFailureOfEitherShrinkingClass(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	watermark := now.Add(-80 * 24 * time.Hour)
	base := workItemsUnit(t, backoffInput(now, watermark, providersync.FailureStreak{}))
	naturalSpan := base.WindowEnd.Sub(*base.WindowStart)
	// time_limit, result_too_large, time_limit in a row: two classes, three halvings.
	streak := providersync.FailureStreakOf([]providersync.UnitOutcome{
		{Failed: true, CauseClass: "time_limit", At: now.Add(-time.Hour)},
		{Failed: true, CauseClass: "result_too_large", At: now.Add(-2 * time.Hour)},
		{Failed: true, CauseClass: "time_limit", At: now.Add(-3 * time.Hour)},
	})
	if streak.ShrinkCount != 3 || streak.Count != 1 {
		t.Fatalf("streak = %+v, want ShrinkCount 3 and Count 1 (three different-class failures are not equal failures)", streak)
	}
	unit := workItemsUnit(t, backoffInput(now, watermark, streak))
	if unit == nil {
		t.Fatal("no unit planned")
	}
	if got := unit.WindowEnd.Sub(*unit.WindowStart); got != naturalSpan/8 {
		t.Fatalf("window = %s, want %s (halved three times)", got, naturalSpan/8)
	}
}

func TestWorkItemsWindowIsNotHalvedForOtherClasses(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	watermark := now.Add(-80 * 24 * time.Hour)
	base := workItemsUnit(t, backoffInput(now, watermark, providersync.FailureStreak{}))
	for _, class := range []string{"provider_rate_limited", "provider_transient", "unclassified", "response_too_large"} {
		streak := providersync.FailureStreakOf([]providersync.UnitOutcome{
			{Failed: true, CauseClass: class, At: now.Add(-time.Hour)},
			{Failed: true, CauseClass: class, At: now.Add(-2 * time.Hour)},
		})
		unit := workItemsUnit(t, backoffInput(now, watermark, streak))
		if unit == nil || !unit.WindowEnd.Equal(*base.WindowEnd) {
			t.Errorf("%s: window end = %v, want the natural end %v", class, unit, base.WindowEnd)
		}
	}
}

func TestWorkItemsSourceIsPlannedOnceADayAfterThreeEqualFailures(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	watermark := now.Add(-10 * 24 * time.Hour)
	for _, class := range []string{"result_too_large", "provider_transient"} {
		recent := providersync.FailureStreak{Count: providersync.FailureStreakThreshold, Class: class, LastAt: now.Add(-time.Hour)}
		if unit := workItemsUnit(t, backoffInput(now, watermark, recent)); unit != nil {
			t.Errorf("%s: planned %+v within a day of the third failure, want no unit", class, unit)
		}
		due := providersync.FailureStreak{Count: providersync.FailureStreakThreshold, Class: class, LastAt: now.Add(-25 * time.Hour)}
		if unit := workItemsUnit(t, backoffInput(now, watermark, due)); unit == nil {
			t.Errorf("%s: no unit planned 25 h after the third failure, want the daily try", class)
		}
		two := providersync.FailureStreak{Count: providersync.FailureStreakThreshold - 1, Class: class, LastAt: now.Add(-time.Hour)}
		if unit := workItemsUnit(t, backoffInput(now, watermark, two)); unit == nil {
			t.Errorf("%s: two failures slowed the source down", class)
		}
	}
}

func TestFailureStreakOfStopsAtASuccessOrAnotherClass(t *testing.T) {
	at := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	failed := func(class string, hoursAgo int) providersync.UnitOutcome {
		return providersync.UnitOutcome{Failed: true, CauseClass: class, At: at.Add(-time.Duration(hoursAgo) * time.Hour)}
	}
	success := providersync.UnitOutcome{At: at.Add(-5 * time.Hour)}
	for _, tc := range []struct {
		name     string
		outcomes []providersync.UnitOutcome
		count    int
		class    string
	}{
		{"none", nil, 0, ""},
		{"success first", []providersync.UnitOutcome{success, failed("time_limit", 6)}, 0, ""},
		{"three equal", []providersync.UnitOutcome{failed("time_limit", 1), failed("time_limit", 2), failed("time_limit", 3)}, 3, "time_limit"},
		{"class changes", []providersync.UnitOutcome{failed("time_limit", 1), failed("result_too_large", 2)}, 1, "time_limit"},
		{"no stored class reads as unclassified", []providersync.UnitOutcome{failed("", 1), failed("", 2), failed("", 3)}, 3, "unclassified"},
		{"invalid stored class reads as unclassified", []providersync.UnitOutcome{failed("Bad Class!", 1), failed("unclassified", 2)}, 2, "unclassified"},
		{"success ends it", []providersync.UnitOutcome{failed("time_limit", 1), failed("time_limit", 2), success, failed("time_limit", 6)}, 2, "time_limit"},
	} {
		got := providersync.FailureStreakOf(tc.outcomes)
		if got.Count != tc.count || got.Class != tc.class {
			t.Errorf("%s: streak = %+v, want count %d class %q", tc.name, got, tc.count, tc.class)
		}
	}
}
