package heatmap

import (
	"reflect"
	"testing"
	"time"
)

func TestMetricFor(t *testing.T) {
	cases := []struct {
		typeValue, metricValue string
		wantOK                 bool
	}{
		{"temporal_load", "review_wait_density", true},
		{"context_switch", "repo_touchpoints", true},
		{"risk", "hotspot_risk", true},
		{"individual", "active_hours", true},
		{"temporal_load", "repo_touchpoints", false},
		{"bogus", "bogus", false},
	}
	for _, tc := range cases {
		_, ok := metricFor(tc.typeValue, tc.metricValue)
		if ok != tc.wantOK {
			t.Errorf("metricFor(%q, %q) ok = %v, want %v", tc.typeValue, tc.metricValue, ok, tc.wantOK)
		}
	}
}

// TestNormalizeRangeDays pins _normalize_range_days's `max(1, min(int(
// range_days or 14), 180))`, including the "0 -> 14" falsy substitution.
func TestNormalizeRangeDays(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, 14},
		{1, 1},
		{14, 14},
		{180, 180},
		{181, 180},
		{1000, 180},
		{-5, 1},
	}
	for _, tc := range cases {
		if got := normalizeRangeDays(tc.in); got != tc.want {
			t.Errorf("normalizeRangeDays(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestClampLimit(t *testing.T) {
	cases := []struct {
		limit, lo, hi, want int
	}{
		{0, 1, 200, 1},
		{-5, 1, 200, 1},
		{50, 1, 200, 50},
		{200, 1, 200, 200},
		{500, 1, 200, 200},
	}
	for _, tc := range cases {
		if got := clampLimit(tc.limit, tc.lo, tc.hi); got != tc.want {
			t.Errorf("clampLimit(%d,%d,%d) = %d, want %d", tc.limit, tc.lo, tc.hi, got, tc.want)
		}
	}
}

func TestParseHourWeekday(t *testing.T) {
	cases := []struct {
		x, y            string
		wantHour, wantW int
		wantValid       bool
	}{
		{"09", "Wed", 9, 3, true},
		{"0", "Mon", 0, 1, true},
		{"23", "Sun", 23, 7, true},
		{"24", "Mon", 0, 0, false},   // hour out of range
		{"-1", "Mon", 0, 0, false},   // hour out of range
		{"abc", "Mon", 0, 0, false},  // unparseable
		{"9", "Funday", 0, 0, false}, // unknown weekday label
	}
	for _, tc := range cases {
		hour, weekday, valid := parseHourWeekday(tc.x, tc.y)
		if valid != tc.wantValid {
			t.Errorf("parseHourWeekday(%q,%q) valid = %v, want %v", tc.x, tc.y, valid, tc.wantValid)
			continue
		}
		if valid && (hour != tc.wantHour || weekday != tc.wantW) {
			t.Errorf("parseHourWeekday(%q,%q) = (%d,%d), want (%d,%d)", tc.x, tc.y, hour, weekday, tc.wantHour, tc.wantW)
		}
	}
}

func TestParseWeekRange(t *testing.T) {
	start, end, valid := parseWeekRange("2024-01-01")
	if !valid {
		t.Fatal("expected valid")
	}
	if !start.Equal(day(2024, 1, 1)) || !end.Equal(day(2024, 1, 8)) {
		t.Fatalf("got (%v,%v)", start, end)
	}
	if _, _, valid := parseWeekRange("not-a-date"); valid {
		t.Fatal("expected invalid")
	}
}

func TestTimeWindowDefaultsToRangeDaysBeforeToday(t *testing.T) {
	startDay, endDay := timeWindow(14, nil, nil)
	if endDay.Sub(startDay) != 14*24*time.Hour {
		t.Fatalf("window = %v, want 14 days", endDay.Sub(startDay))
	}
}

func TestTimeWindowExplicitDates(t *testing.T) {
	start := day(2024, 1, 1)
	end := day(2024, 1, 10)
	gotStart, gotEnd := timeWindow(14, &start, &end)
	if !gotStart.Equal(start) {
		t.Fatalf("start = %v, want %v", gotStart, start)
	}
	if want := day(2024, 1, 11); !gotEnd.Equal(want) {
		t.Fatalf("end = %v, want %v (end_date+1)", gotEnd, want)
	}
}

func TestTimeWindowStartNotBeforeEndClampsToOneDay(t *testing.T) {
	start := day(2024, 1, 15)
	end := day(2024, 1, 10)
	gotStart, gotEnd := timeWindow(14, &start, &end)
	wantEnd := day(2024, 1, 11)
	wantStart := day(2024, 1, 10)
	if !gotStart.Equal(wantStart) || !gotEnd.Equal(wantEnd) {
		t.Fatalf("got (%v,%v), want (%v,%v)", gotStart, gotEnd, wantStart, wantEnd)
	}
}

func TestFormatAxisValueHour(t *testing.T) {
	if got := formatAxisValue("hour", 3); got != "03" {
		t.Fatalf("got %q, want %q", got, "03")
	}
	if got := formatAxisValue("hour", 23); got != "23" {
		t.Fatalf("got %q, want %q", got, "23")
	}
}

func TestFormatAxisValueWeekday(t *testing.T) {
	if got := formatAxisValue("weekday", 1); got != "Mon" {
		t.Fatalf("got %q, want Mon", got)
	}
	if got := formatAxisValue("weekday", 7); got != "Sun" {
		t.Fatalf("got %q, want Sun", got)
	}
	// Out of range falls back to Python's plain str(value).
	if got := formatAxisValue("weekday", 8); got != "8" {
		t.Fatalf("got %q, want 8", got)
	}
}

func TestFormatAxisValueDayWeek(t *testing.T) {
	if got := formatAxisValue("day", day(2024, 3, 5)); got != "2024-03-05" {
		t.Fatalf("got %q", got)
	}
	if got := formatAxisValue("week", day(2024, 3, 5)); got != "2024-03-05" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatAxisValueDefaultAndNil(t *testing.T) {
	if got := formatAxisValue("repo", "my-repo"); got != "my-repo" {
		t.Fatalf("got %q", got)
	}
	if got := formatAxisValue("hour", nil); got != "" {
		t.Fatalf("got %q, want empty for nil", got)
	}
}

// TestAxisOrderHourIsAlwaysFullFixedList pins that the hour axis is
// ALWAYS the full 00..23 list, ignoring which hours actually appear in
// the data (services/heatmap.py:125-126).
func TestAxisOrderHourIsAlwaysFullFixedList(t *testing.T) {
	got := axisOrder("hour", []string{"09"}, map[string]float64{"09": 1})
	if !reflect.DeepEqual(got, hourLabels()) {
		t.Fatalf("got %v, want the full hour list", got)
	}
}

// TestAxisOrderWeekdayFiltersToPresentLabels pins the weekday axis
// filtering to only present labels, in Mon..Sun order.
func TestAxisOrderWeekdayFiltersToPresentLabels(t *testing.T) {
	got := axisOrder("weekday", []string{"Wed", "Mon", "Wed"}, nil)
	if want := []string{"Mon", "Wed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestAxisOrderWeekdayFallsBackToFullListWhenNoneMatch pins the "or
// _weekday_labels()" fallback (services/heatmap.py:128-130): a values
// list containing only out-of-range labels (never a WEEKDAY_LABELS
// member) yields the full Mon..Sun list.
func TestAxisOrderWeekdayFallsBackToFullListWhenNoneMatch(t *testing.T) {
	got := axisOrder("weekday", []string{"8", "9"}, nil)
	if !reflect.DeepEqual(got, weekdayLabels) {
		t.Fatalf("got %v, want the full weekday list", got)
	}
}

// TestAxisOrderDayWeekSortsPlainISO pins the plain lexicographic sort
// (which is also chronological for ISO dates).
func TestAxisOrderDayWeekSortsPlainISO(t *testing.T) {
	got := axisOrder("day", []string{"2024-02-01", "2024-01-01", "2024-01-15"}, nil)
	want := []string{"2024-01-01", "2024-01-15", "2024-02-01"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestAxisOrderDefaultSortsByTotalDescending pins the fallback branch
// (repo/file kinds) used for everything not hour/weekday/day/week/
// status/age_bucket.
func TestAxisOrderDefaultSortsByTotalDescending(t *testing.T) {
	totals := map[string]float64{"a": 5, "b": 20, "c": 10}
	got := axisOrder("repo", []string{"a", "b", "c"}, totals)
	want := []string{"b", "c", "a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestAxisOrderDedupesPreservingFirstOccurrence pins values_list's own
// dict.fromkeys-style de-dup (order of first occurrence).
func TestAxisOrderDedupesPreservingFirstOccurrence(t *testing.T) {
	got := axisOrder("repo", []string{"a", "b", "a", "c"}, map[string]float64{})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestCellsFromRowsDropsEmptyLabels pins _cells_from_rows's own
// `if x_label and y_label` guard.
func TestCellsFromRowsDropsEmptyLabels(t *testing.T) {
	rows := []metricRow{
		{X: 3, Y: "repoA", Value: 1.0},
		{X: nil, Y: "repoB", Value: 2.0}, // nil X -> "" -> dropped
	}
	got := cellsFromRows(rows, "hour", "repo")
	want := []Cell{{X: "03", Y: "repoA", Value: 1.0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestValidScopeLevels pins ScopeFilter.level's Literal set exactly.
func TestValidScopeLevels(t *testing.T) {
	want := map[string]bool{"org": true, "team": true, "repo": true, "service": true, "developer": true}
	if !reflect.DeepEqual(validScopeLevels, want) {
		t.Fatalf("validScopeLevels = %v, want %v", validScopeLevels, want)
	}
	if validScopeLevels["person"] {
		t.Fatal(`"person" must NOT be a valid heatmap scope level (unlike quadrant's own scope set)`)
	}
}
