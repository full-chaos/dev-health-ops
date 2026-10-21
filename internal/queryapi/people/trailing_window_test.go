package people

import (
	"testing"
	"time"
)

// TestTimeWindowTrailingExcludesToday pins that the people window is the shared trailing window [today-N, today)
// in UTC at any time of day.
func TestTimeWindowTrailingExcludesToday(t *testing.T) {
	today := time.Date(2026, time.October, 18, 0, 0, 0, 0, time.UTC)
	for _, offset := range []time.Duration{time.Second, 12 * time.Hour, 24*time.Hour - time.Second} {
		start, end, _, _, err := timeWindow(today.Add(offset), 7, 7)
		if err != nil {
			t.Fatal(err)
		}
		if !end.Equal(today) || !start.Equal(today.AddDate(0, 0, -7)) {
			t.Errorf("offset %v: [%v, %v), want [%v, %v)", offset, start, end, today.AddDate(0, 0, -7), today)
		}
	}
}
