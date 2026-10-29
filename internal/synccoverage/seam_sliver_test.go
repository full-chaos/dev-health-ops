package synccoverage

import (
	"testing"
	"time"
)

// CHAOS-11774: the seam where one day-bounded window ends at 23:59:59.999999 and the next starts at 00:00:00 is not a
// failed range. streamCompactWindows removes each later success from the failed set one at a time, so a wide failed
// unit followed by weekly success windows used to keep a 1-microsecond remainder at every weekly seam.
func TestSubtractingAdjacentSuccessWindowsLeavesNoMicrosecondSeamOfAFailedRange(t *testing.T) {
	day := func(m time.Month, d int) time.Time { return time.Date(2026, m, d, 0, 0, 0, 0, time.UTC) }
	failed := []coverageInterval{{Since: day(8, 26), Before: day(9, 16)}}
	for _, success := range []coverageInterval{
		{Since: day(8, 26), Before: day(9, 2).Add(-time.Microsecond)}, // 23:59:59.999999
		{Since: day(9, 2), Before: day(9, 9).Add(-time.Microsecond)},
		{Since: day(9, 9), Before: day(9, 16)},
	} {
		failed = subtractIntervals(failed, []coverageInterval{success})
	}
	if len(failed) != 0 {
		t.Fatalf("failed ranges left after the whole span was covered: %+v", failed)
	}
}

func TestARemainderWiderThanTheToleranceIsStillAFailedRange(t *testing.T) {
	day := func(m time.Month, d int) time.Time { return time.Date(2026, m, d, 0, 0, 0, 0, time.UTC) }
	failed := []coverageInterval{{Since: day(8, 26), Before: day(9, 16)}}
	failed = subtractIntervals(failed, []coverageInterval{{Since: day(8, 26), Before: day(9, 2).Add(-2 * time.Microsecond)}})
	failed = subtractIntervals(failed, []coverageInterval{{Since: day(9, 2), Before: day(9, 16)}})
	if len(failed) != 1 || failed[0].Before.Sub(failed[0].Since) != 2*time.Microsecond {
		t.Fatalf("a 2-microsecond hole is a real remainder: %+v", failed)
	}
}

// A zero-width interval (since == before) is no interval at all, failed or not.
func TestAZeroWidthFailedIntervalIsNeverARange(t *testing.T) {
	at := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	if got := mergeIntervals([]coverageInterval{{Since: at, Before: at}}); len(got) != 0 {
		t.Fatalf("a zero-width interval survived mergeIntervals: %+v", got)
	}
	if got := subtractIntervals([]coverageInterval{{Since: at, Before: at}}, nil); len(got) != 0 {
		t.Fatalf("a zero-width interval survived subtractIntervals: %+v", got)
	}
}
