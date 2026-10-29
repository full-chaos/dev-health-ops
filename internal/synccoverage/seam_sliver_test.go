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

// The prod shape (CHAOS-11774): a wide failed unit whose first week was never covered, followed by weekly success
// windows that end at 23:59:59.999999 and restart at 00:00:00. The real failed week stays; no seam sliver remains.
func TestAWideFailedUnitKeepsItsRealFailedRangeAndLosesTheWeeklySeams(t *testing.T) {
	at := func(m time.Month, d, h, min, s, us int) time.Time {
		return time.Date(2026, m, d, h, min, s, us*1000, time.UTC)
	}
	failed := []coverageInterval{{Since: at(8, 19, 13, 1, 58, 246840), Before: at(9, 16, 0, 0, 0, 0)}}
	for _, success := range []coverageInterval{
		{Since: at(8, 26, 0, 0, 0, 0), Before: at(9, 1, 23, 59, 59, 999999)},
		{Since: at(9, 2, 0, 0, 0, 0), Before: at(9, 8, 23, 59, 59, 999999)},
		{Since: at(9, 9, 0, 0, 0, 0), Before: at(9, 16, 0, 0, 0, 0)},
	} {
		failed = subtractIntervals(failed, []coverageInterval{success})
	}
	if len(failed) != 1 || !failed[0].Since.Equal(at(8, 19, 13, 1, 58, 246840)) || !failed[0].Before.Equal(at(8, 26, 0, 0, 0, 0)) {
		t.Fatalf("want exactly the real failed range 08-19 13:01:58.24684 .. 08-26, got %+v", failed)
	}
}

// The cicd shape: one failed week whose success covers it up to 23:59:59.999999 leaves only the seam, which is not a range.
func TestACicdFailedWeekCoveredUpToTheSeamLeavesNoRange(t *testing.T) {
	since := time.Date(2026, 8, 19, 13, 1, 58, 246840000, time.UTC)
	end := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	failed := subtractIntervals([]coverageInterval{{Since: since, Before: end}}, []coverageInterval{{Since: since, Before: end.Add(-time.Microsecond)}})
	if len(failed) != 0 {
		t.Fatalf("a 1-microsecond seam is a failed range: %+v", failed)
	}
	whole := subtractIntervals([]coverageInterval{{Since: since, Before: end}}, nil)
	if len(whole) != 1 {
		t.Fatalf("an uncovered failed week is still one failed range: %+v", whole)
	}
}
