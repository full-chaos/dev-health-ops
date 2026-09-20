package aianalytics

import (
	"testing"
	"time"
)

// TestDayBoundsMatchTheReferenceBoundParameters pins the window edges to the
// text the reference's ClickHouse client binds for a DateTime parameter built
// from midnight of the first day and the last microsecond of the last day:
// second precision, so the fraction of the last second is dropped.
func TestDayBoundsMatchTheReferenceBoundParameters(t *testing.T) {
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 12, 30, 0, 0, time.UTC) }
	start, end := dayBounds(day(2026, 1, 1), day(2026, 1, 5))
	const layout = "2006-01-02 15:04:05"
	if got, want := start.Format(layout), "2026-01-01 00:00:00"; got != want {
		t.Errorf("start = %q, want %q", got, want)
	}
	if got, want := end.Format(layout), "2026-01-05 23:59:59"; got != want {
		t.Errorf("end = %q, want %q", got, want)
	}
	if end.Nanosecond() != 0 {
		t.Errorf("end carries a fractional second: %d ns", end.Nanosecond())
	}
}
