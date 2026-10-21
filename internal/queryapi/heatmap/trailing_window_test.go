package heatmap

import (
	"testing"
	"time"
)

// TestTimeWindowTrailingExcludesToday pins that a request with no end date reads the shared trailing window
// [today-N, today) in UTC.
func TestTimeWindowTrailingExcludesToday(t *testing.T) {
	before := time.Now().UTC()
	start, end, err := timeWindow(7, nil, nil)
	after := time.Now().UTC()
	if err != nil {
		t.Fatal(err)
	}
	if before.Format(time.DateOnly) != after.Format(time.DateOnly) {
		t.Skip("clock crossed UTC midnight during the test")
	}
	today := time.Date(before.Year(), before.Month(), before.Day(), 0, 0, 0, 0, time.UTC)
	if !end.Equal(today) || !start.Equal(today.AddDate(0, 0, -7)) {
		t.Fatalf("[%v, %v), want [%v, %v)", start, end, today.AddDate(0, 0, -7), today)
	}
}
