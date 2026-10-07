package syncdispatchruntime

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func unitWindowTestDays(first time.Time, count int) []time.Time {
	days := make([]time.Time, 0, count)
	for offset := 0; offset < count; offset++ {
		days = append(days, first.AddDate(0, 0, offset))
	}
	return days
}

// The days the fan-out records for the window of a work-items unit are the
// days the route validated the unit for: the same function gives both. The
// expected days are written out here, for the four providers and both unit
// kinds, so a change of the function in either direction fails.
func TestWorkItemUnitsWindowDaysAreTheDaysOfTheRouteWindowRule(t *testing.T) {
	runStartedAt := time.Date(2026, 9, 20, 10, 5, 0, 0, time.UTC)
	now := runStartedAt.Add(20 * time.Minute)
	at := func(month time.Month, day, hour int) *time.Time {
		value := time.Date(2026, month, day, hour, 0, 0, 0, time.UTC)
		return &value
	}
	date := func(month time.Month, day int) time.Time {
		return time.Date(2026, month, day, 0, 0, 0, 0, time.UTC)
	}
	kinds := []struct {
		name          string
		since, before *time.Time
		want          []time.Time
	}{
		// An hourly unit inside one day: that day.
		{"incremental", at(9, 20, 9), at(9, 20, 10), []time.Time{date(9, 20)}},
		// An hourly unit over midnight: both days.
		{"incremental over midnight", at(9, 19, 23), at(9, 20, 1), []time.Time{date(9, 19), date(9, 20)}},
		// `before` is exclusive: a window that ends at midnight does not
		// reach the next day.
		{"incremental to midnight", at(9, 19, 23), at(9, 20, 0), []time.Time{date(9, 19)}},
		// A backfill chunk of seven days (the planner's default chunk).
		{"backfill", at(6, 1, 0), at(6, 8, 0), unitWindowTestDays(date(6, 1), 7)},
		// The widest window the route accepts.
		{"backfill at the bound", at(1, 1, 0), at(1, 1, 0), nil},
	}
	last := time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC)
	kinds[4].before = &last
	kinds[4].want = unitWindowTestDays(date(1, 1), 366)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		capability, ok := providersync.Capability(provider, "work-items")
		if !ok || !slices.Contains(capability.LegacyTargets, "work-items") {
			t.Fatalf("%s work-items: capability ok=%v targets=%v; the plan takes the unit windows of the work-items target",
				provider, ok, capability.LegacyTargets)
		}
		for _, kind := range kinds {
			t.Run(provider+"/"+kind.name, func(t *testing.T) {
				got, err := workItemUnitsWindowDays(
					[]workItemUnitWindow{{since: kind.since, before: kind.before}}, runStartedAt, now,
				)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, kind.want) {
					t.Fatalf("recorded window days\n got %v\nwant %v", got, kind.want)
				}
				route, err := providersync.WorkItemsUnitWindowDays(kind.since, kind.before, runStartedAt)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, route) {
					t.Fatalf("recorded window days differ from the days of the route window rule\n got %v\nrule %v", got, route)
				}
				t.Logf("marked window days = %d", len(got))
			})
		}
	}
}

func TestWorkItemUnitsWindowDaysOfARun(t *testing.T) {
	runStartedAt := time.Date(2026, 9, 20, 23, 50, 0, 0, time.UTC)
	now := time.Date(2026, 9, 21, 0, 10, 0, 0, time.UTC)
	at := func(day, hour int) *time.Time {
		value := time.Date(2026, 9, day, hour, 0, 0, 0, time.UTC)
		return &value
	}
	date := func(day int) time.Time { return time.Date(2026, 9, day, 0, 0, 0, 0, time.UTC) }

	t.Run("no work-items unit records no window day", func(t *testing.T) {
		got, err := workItemUnitsWindowDays(nil, runStartedAt, now)
		if err != nil || got != nil {
			t.Fatalf("days=%v err=%v", got, err)
		}
	})
	t.Run("the days of several units are distinct and ascend", func(t *testing.T) {
		got, err := workItemUnitsWindowDays([]workItemUnitWindow{
			{since: at(12, 0), before: at(14, 0)},
			{since: at(10, 0), before: at(13, 0)},
			{since: at(13, 6), before: at(13, 7)},
		}, runStartedAt, now)
		want := []time.Time{date(10), date(11), date(12), date(13)}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("days=%v err=%v want %v", got, err, want)
		}
	})
	// A unit with no `before` ended its window on the day of its own clock,
	// which is between the start of the run and now. Both days are recorded,
	// so the day of the unit's clock is among them whichever it was.
	t.Run("no before: since to the run start, and every day of the run", func(t *testing.T) {
		got, err := workItemUnitsWindowDays([]workItemUnitWindow{{since: at(18, 12)}}, runStartedAt, now)
		want := []time.Time{date(18), date(19), date(20), date(21)}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("days=%v err=%v want %v", got, err, want)
		}
	})
	t.Run("no since and no before: every day of the run", func(t *testing.T) {
		got, err := workItemUnitsWindowDays([]workItemUnitWindow{{}}, runStartedAt, now)
		want := []time.Time{date(20), date(21)}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("days=%v err=%v want %v", got, err, want)
		}
	})
	// A successful unit passed the route's window rule. A stored window that
	// the rule refuses is a failure of the fan-out, never "no day".
	t.Run("a window the route refuses fails the plan", func(t *testing.T) {
		for name, unit := range map[string]workItemUnitWindow{
			"since after before": {since: at(14, 0), before: at(12, 0)},
			"over the bound": {since: func() *time.Time {
				value := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
				return &value
			}(), before: at(14, 0)},
		} {
			got, err := workItemUnitsWindowDays([]workItemUnitWindow{unit}, runStartedAt, now)
			if !errors.Is(err, ErrPostSyncUnavailable) || got != nil {
				t.Fatalf("%s: days=%v err=%v want ErrPostSyncUnavailable", name, got, err)
			}
		}
	})
	t.Run("a run with no start time fails the plan", func(t *testing.T) {
		if _, err := workItemUnitsWindowDays([]workItemUnitWindow{{since: at(12, 0), before: at(13, 0)}}, time.Time{}, now); !errors.Is(err, ErrPostSyncUnavailable) {
			t.Fatalf("err=%v want ErrPostSyncUnavailable", err)
		}
	})
}
