package server

import (
	"testing"
	"time"
)

// TestFiltersTimeWindowExcludesToday pins that the one reader window of REST investment, sunburst, sankey and
// explain (timeWindow -> filtersTimeWindow) is the shared trailing window [today-N, today) in UTC when no end date is
// given, not a window of its own.
func TestFiltersTimeWindowExcludesToday(t *testing.T) {
	for _, n := range []int{1, 7, 30} {
		before := time.Now().UTC()
		start, end, err := timeWindow(map[string]any{"time": map[string]any{"range_days": float64(n)}})
		after := time.Now().UTC()
		if err != nil {
			t.Fatal(err)
		}
		if before.Format(time.DateOnly) != after.Format(time.DateOnly) {
			t.Skip("clock crossed UTC midnight during the test")
		}
		today := time.Date(before.Year(), before.Month(), before.Day(), 0, 0, 0, 0, time.UTC)
		if !end.Equal(today) || !start.Equal(today.AddDate(0, 0, -n)) {
			t.Errorf("N %d: [%v, %v), want [%v, %v)", n, start, end, today.AddDate(0, 0, -n), today)
		}
	}
}
