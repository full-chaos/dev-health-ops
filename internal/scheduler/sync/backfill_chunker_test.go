package sync

import (
	"errors"
	"testing"
	"time"
)

// Golden cases mirror tests/test_backfill.py's test_chunk_date_range_*
// functions exactly, so this port's behavior is provably identical to
// Python's chunk_date_range without needing a live-Python oracle for
// something this small and stable.

func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func TestChunkDateRangeSingleDay(t *testing.T) {
	since := date(2026, 1, 10)
	before := date(2026, 1, 10)
	got, err := ChunkDateRange(since, before, 7)
	if err != nil {
		t.Fatal(err)
	}
	want := []DateWindow{{Since: since, Before: before}}
	assertDateWindowsEqual(t, got, want)
}

func TestChunkDateRangeExactlySevenDays(t *testing.T) {
	since := date(2026, 1, 1)
	before := date(2026, 1, 7)
	got, err := ChunkDateRange(since, before, 7)
	if err != nil {
		t.Fatal(err)
	}
	want := []DateWindow{{Since: since, Before: before}}
	assertDateWindowsEqual(t, got, want)
}

func TestChunkDateRangeTenDaysCreatesTwoChunks(t *testing.T) {
	got, err := ChunkDateRange(date(2026, 1, 1), date(2026, 1, 10), 7)
	if err != nil {
		t.Fatal(err)
	}
	want := []DateWindow{
		{Since: date(2026, 1, 1), Before: date(2026, 1, 7)},
		{Since: date(2026, 1, 8), Before: date(2026, 1, 10)},
	}
	assertDateWindowsEqual(t, got, want)
}

func TestChunkDateRangeEmptyRangeErrors(t *testing.T) {
	_, err := ChunkDateRange(date(2026, 1, 11), date(2026, 1, 10), 7)
	if !errors.Is(err, ErrInvalidDateRange) {
		t.Fatalf("ChunkDateRange() error = %v, want ErrInvalidDateRange", err)
	}
}

func TestChunkDateRangeRejectsNonPositiveChunkDays(t *testing.T) {
	for _, chunkDays := range []int{0, -1} {
		_, err := ChunkDateRange(date(2026, 1, 1), date(2026, 1, 10), chunkDays)
		if !errors.Is(err, ErrInvalidChunkDays) {
			t.Fatalf("ChunkDateRange() with chunkDays=%d error = %v, want ErrInvalidChunkDays", chunkDays, err)
		}
	}
}

// TestChunkDateRangeAcceptsAnUnboundedChunkDaysLikePython is the codex
// gate-round-10 fix (a recurring finding across rounds 2/3/9/10, finally
// closed at the class): Python's LINEAR_BACKFILL_MAX_WINDOW_DAYS has no
// upper bound at all, only `value > 0`. Every prior fix here picked a new
// finite "largest safe" constant (3650 -> 106751 -> 106752), and each one
// was still a ceiling some Python-accepted value could exceed -- codex
// gate round 10 found exactly that: 106753 (one more than the round-9
// fix's own boundary) was Python-valid, Go-rejected. Switching
// ChunkDateRange to time.Time.AddDate (day-count arithmetic, no
// int64-nanosecond ceiling) removed the finite bound entirely instead of
// chasing it again; this proves Go now accepts values far beyond every
// previous "boundary" this bug has had, including the exact codex-round-9
// (106752) and round-10 (106753) repro values, and an arbitrary, far
// larger one for good measure.
func TestChunkDateRangeAcceptsAnUnboundedChunkDaysLikePython(t *testing.T) {
	since := date(2026, 8, 1)
	before := date(2026, 8, 20)
	for _, chunkDays := range []int{106752, 106753, 1 << 30} {
		got, err := ChunkDateRange(since, before, chunkDays)
		if err != nil {
			t.Fatalf("ChunkDateRange() with chunkDays=%d error = %v, want nil", chunkDays, err)
		}
		want := []DateWindow{{Since: since, Before: before}}
		assertDateWindowsEqual(t, got, want)
	}
}

func TestChunkDateRangeTruncatesToUTCCalendarDate(t *testing.T) {
	// A time-of-day component (as scheduled_for/since/before would carry as
	// timestamptz values in production, unlike Python's date-only chunker
	// inputs) must not shift chunk boundaries -- only the UTC calendar date
	// matters, matching Python's date() semantics exactly.
	since := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	before := time.Date(2026, 1, 10, 0, 0, 1, 0, time.UTC)
	got, err := ChunkDateRange(since, before, 7)
	if err != nil {
		t.Fatal(err)
	}
	want := []DateWindow{
		{Since: date(2026, 1, 1), Before: date(2026, 1, 7)},
		{Since: date(2026, 1, 8), Before: date(2026, 1, 10)},
	}
	assertDateWindowsEqual(t, got, want)
}

func assertDateWindowsEqual(t *testing.T, got, want []DateWindow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ChunkDateRange() = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Since.Equal(want[i].Since) || !got[i].Before.Equal(want[i].Before) {
			t.Fatalf("ChunkDateRange()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
