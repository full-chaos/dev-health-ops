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

func TestChunkDateRangeRejectsChunkDaysThatWouldOverflowDuration(t *testing.T) {
	// Codex review (round 2, P2): a chunkDays value large enough that
	// (chunkDays-1)*24h overflows time.Duration's int64-nanosecond range
	// must be rejected outright, not silently wrap into a negative span
	// that walks the cursor backward forever.
	for _, chunkDays := range []int{maxChunkDays + 1, 1 << 30} {
		_, err := ChunkDateRange(date(2026, 1, 1), date(2026, 1, 10), chunkDays)
		if !errors.Is(err, ErrInvalidChunkDays) {
			t.Fatalf("ChunkDateRange() with chunkDays=%d error = %v, want ErrInvalidChunkDays", chunkDays, err)
		}
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
