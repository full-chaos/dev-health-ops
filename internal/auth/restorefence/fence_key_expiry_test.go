package restorefence

import (
	"testing"
	"time"
)

// liveKeyExpiry is an expiry that stays ahead of the clock the restore reads (clock_timestamp()), so a fence test never
// depends on the date it runs, and strictly later than `after`. It is whole seconds, so the column round-trips it exactly.
func liveKeyExpiry(now, after time.Time) time.Time {
	long := now.UTC().AddDate(5, 0, 0).Truncate(time.Second)
	if floor := after.Add(time.Second); long.Before(floor) {
		return floor
	}
	return long
}

func TestLiveKeyExpiryStaysAheadOfTheClock(t *testing.T) {
	old := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	short := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for _, now := range []time.Time{
		time.Date(2021, 11, 1, 23, 59, 59, 999999999, time.UTC),
		time.Date(2021, 11, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 17, 12, 0, 0, 123456789, time.UTC),
		time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2031, 6, 1, 0, 0, 0, 0, time.UTC),
	} {
		got := liveKeyExpiry(now, short)
		if !got.After(now) || !got.After(short) || got.Nanosecond() != 0 {
			t.Errorf("clock %s: expiry %s is not ahead of the clock and short in whole seconds", now, got)
		}
	}
	if past := time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC); old.After(past) {
		t.Errorf("the old fixed expiry %s is still live at %s", old, past)
	}
}
