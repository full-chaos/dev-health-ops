package workitemmetrics

import (
	"reflect"
	"testing"
	"time"
)

var capabilityDay = time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)

func capabilityAt(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 12, 0, 0, 0, time.UTC)
}

func capabilityTimePtr(value time.Time) *time.Time { return &value }

func capabilityPoints(value float64) *float64 { return &value }

func TestCapabilityWindowIsTheDayAndThe89DaysBefore(t *testing.T) {
	first, last := CapabilityWindow(capabilityDay)
	if want := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC); !last.Equal(want) {
		t.Fatalf("last = %s, want %s", last, want)
	}
	if want := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC); !first.Equal(want) {
		t.Fatalf("first = %s, want %s", first, want)
	}
	if days := int(last.Sub(first).Hours()/24) + 1; days != CapabilityWindowDays {
		t.Fatalf("window holds %d days, want %d", days, CapabilityWindowDays)
	}
}

// Each edge of the item rule: created on the day after the window, done and
// completed the day before the window, done without a completion time, done
// and completed on the first day, open and created long before the window.
func TestInCapabilityWindowEdges(t *testing.T) {
	first, last := CapabilityWindow(capabilityDay)
	cases := []struct {
		name string
		item Item
		want bool
	}{
		{"created after the last day", Item{CreatedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Status: "todo"}, false},
		{"created at the end of the last day", Item{CreatedAt: time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC), Status: "todo"}, true},
		{"done before the first day", Item{CreatedAt: capabilityAt(2026, 1, 1), Status: "done", CompletedAt: capabilityTimePtr(time.Date(2026, 7, 2, 23, 59, 59, 0, time.UTC))}, false},
		{"done on the first day", Item{CreatedAt: capabilityAt(2026, 1, 1), Status: "done", CompletedAt: capabilityTimePtr(time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))}, true},
		{"done without a completion time", Item{CreatedAt: capabilityAt(2026, 9, 1), Status: "done"}, false},
		{"open since long before the window", Item{CreatedAt: capabilityAt(2024, 1, 1), Status: "in_progress"}, true},
		{"canceled long ago is not done", Item{CreatedAt: capabilityAt(2024, 1, 1), Status: "canceled", ClosedAt: capabilityTimePtr(capabilityAt(2024, 2, 1))}, true},
	}
	for _, tc := range cases {
		if got := InCapabilityWindow(tc.item, first, last); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDeriveMeasureCapabilityPerProvider(t *testing.T) {
	inWindow := capabilityAt(2026, 9, 20)
	items := []Item{
		{Provider: "jira", Type: "story", Status: "todo", CreatedAt: inWindow, StoryPoints: capabilityPoints(0)},
		{Provider: "jira", Type: "bug", Status: "todo", CreatedAt: inWindow},
		{Provider: "github", Type: "issue", Status: "todo", CreatedAt: inWindow},
		{Provider: "github", Type: "bug", Status: "done", CreatedAt: inWindow, CompletedAt: capabilityTimePtr(capabilityAt(2026, 1, 2))},
		{Provider: "github", Type: "story", Status: "todo", CreatedAt: capabilityAt(2026, 10, 2), StoryPoints: capabilityPoints(3)},
		{Provider: "gitlab", Type: "issue", Status: "done", CreatedAt: inWindow, CompletedAt: capabilityTimePtr(inWindow), StoryPoints: capabilityPoints(2)},
		{Provider: "gitlab", Type: "issue", Status: "todo", CreatedAt: inWindow, StoryPoints: capabilityPoints(5)},
	}
	first, last := CapabilityWindow(capabilityDay)
	row := func(provider, measure string, tracked bool, evidence, count int) CapabilityRow {
		return CapabilityRow{
			Provider: provider, Measure: measure, Tracked: tracked,
			EvidenceCount: evidence, ItemCount: count, WindowStart: first, WindowEnd: last,
		}
	}
	want := []CapabilityRow{
		row("github", MeasureBugCompletedRatio, false, 0, 1),
		row("github", MeasureStoryPointsCompleted, false, 0, 1),
		row("gitlab", MeasureBugCompletedRatio, false, 0, 2),
		row("gitlab", MeasureStoryPointsCompleted, true, 2, 2),
		row("jira", MeasureBugCompletedRatio, true, 1, 2),
		row("jira", MeasureStoryPointsCompleted, true, 1, 2),
	}
	if got := DeriveMeasureCapability(capabilityDay, items); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %+v\nwant %+v", got, want)
	}
}

// A provider without an item in the window has no row: absent is unknown.
func TestDeriveMeasureCapabilityWritesNoRowWithoutItems(t *testing.T) {
	outside := []Item{
		{Provider: "linear", Type: "bug", Status: "done", CreatedAt: capabilityAt(2025, 1, 1),
			CompletedAt: capabilityTimePtr(capabilityAt(2025, 2, 1)), StoryPoints: capabilityPoints(1)},
	}
	if got := DeriveMeasureCapability(capabilityDay, outside); len(got) != 0 {
		t.Fatalf("rows = %+v, want none", got)
	}
	if got := DeriveMeasureCapability(capabilityDay, nil); len(got) != 0 {
		t.Fatalf("rows for no items = %+v, want none", got)
	}
}
