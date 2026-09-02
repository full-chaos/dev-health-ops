package chquery

import (
	"testing"
	"time"
)

// TestNormalizeTimestampConvertsRatherThanReinterprets pins the fix for
// CHAOS-4441 plan section 5f, using the epochs measured against a real
// ClickHouse container whose server timezone was Asia/Kolkata (+05:30).
//
// PR2 shipped normalizeTimestamp as a wall-clock REBUILD, modelling Python's
// `.replace(tzinfo=utc)`. Measured, the driver's behaviour is the inverse of
// what the column declarations suggest:
//
//	DateTime64(3)   (no tz)   python driver returns AWARE, in the SERVER tz
//	DateTime64(3,'UTC')       python driver returns NAIVE
//
// so _ensure_utc CONVERTS the first and REINTERPRETS the second -- and the
// rebuild was wrong for work_items, the naive-declared table, by the server's
// UTC offset.
//
// The offset used here is +05:30 deliberately: a half-hour zone cannot be
// confused with an hour-boundary rounding, and it makes an accidental
// hour-granular fix visible.
func TestNormalizeTimestampConvertsRatherThanReinterprets(t *testing.T) {
	kolkata := time.FixedZone("IST", 5*3600+30*60)

	// The row seeded at '2026-09-02 10:30:00' in a DateTime64(3) column, as the
	// Go driver returns it: wall clock 10:30 with the server's location.
	scanned := time.Date(2026, 9, 2, 10, 30, 0, 0, kolkata)

	// Python's _ensure_utc on the same row, measured: 05:00Z.
	const pythonEpochMilli = 1788325200000

	got := normalizeTimestamp(scanned)
	if got.UnixMilli() != pythonEpochMilli {
		t.Errorf("normalizeTimestamp(%v) = %v (epoch_ms %d), python _ensure_utc "+
			"gives epoch_ms %d -- a %v difference.\n"+
			"This is the PR2 defect: rebuilding the wall clock in UTC REINTERPRETS "+
			"the instant instead of converting it, so every work_items timestamp "+
			"shifts by the ClickHouse server's UTC offset.",
			scanned, got, got.UnixMilli(), pythonEpochMilli,
			time.Duration(got.UnixMilli()-pythonEpochMilli)*time.Millisecond)
	}
	if got.Location() != time.UTC {
		t.Errorf("result location = %v, want UTC", got.Location())
	}

	// The wall clock MUST move. If it does not, the conversion silently became a
	// reinterpretation again.
	if got.Hour() == scanned.Hour() && got.Minute() == scanned.Minute() {
		t.Errorf("wall clock unchanged (%02d:%02d): normalizeTimestamp is "+
			"reinterpreting, not converting", got.Hour(), got.Minute())
	}

	// A value already on UTC must be unchanged -- this is the DateTime64(3,'UTC')
	// column, where converting and reinterpreting coincide and where PR2's
	// version was correct. It must stay correct.
	alreadyUTC := time.Date(2026, 9, 2, 10, 30, 0, 0, time.UTC)
	if got := normalizeTimestamp(alreadyUTC); got.UnixMilli() != 1788345000000 {
		t.Errorf("a UTC value must pass through unchanged: got epoch_ms %d, want %d",
			got.UnixMilli(), 1788345000000)
	}
}

// TestNormalizeTimestampIsIndependentOfClientTimezone pins why converting also
// removes a deployment dependency.
//
// With clickhouse-connect's apply_server_timezone=False the Python driver
// attaches the CLIENT's local zone rather than the server's -- measured, it
// returned America/Los_Angeles on this machine. _ensure_utc still recovered the
// same instant, because converting never reads the wall clock. A reinterpreting
// port would have been wrong by a PER-MACHINE offset, which is worse than being
// wrong by a fixed one: it would reproduce differently for each developer.
func TestNormalizeTimestampIsIndependentOfClientTimezone(t *testing.T) {
	const instant = 1788325200000 // 2026-09-02T05:00:00Z

	for _, zone := range []struct {
		name   string
		offset int
	}{
		{"UTC", 0},
		{"Asia/Kolkata", 5*3600 + 30*60},
		{"America/Los_Angeles", -7 * 3600},
		{"Pacific/Chatham", 12*3600 + 45*60}, // +12:45, the most awkward real zone
		{"Etc/GMT-14", 14 * 3600},
	} {
		t.Run(zone.name, func(t *testing.T) {
			// The same INSTANT, presented in a different location -- which is
			// exactly what varying apply_server_timezone does.
			presented := time.UnixMilli(instant).In(time.FixedZone(zone.name, zone.offset))
			if got := normalizeTimestamp(presented); got.UnixMilli() != instant {
				t.Errorf("presented in %s the instant became epoch_ms %d, want %d -- "+
					"the result must not depend on which zone the driver attached",
					zone.name, got.UnixMilli(), instant)
			}
		})
	}
}

// TestNormalizeOptionalTimestampSharesTheFix guards the nullable wrapper, which
// is what actually carries completed_at, merged_at and closed_at.
func TestNormalizeOptionalTimestampSharesTheFix(t *testing.T) {
	if normalizeOptionalTimestamp(nil) != nil {
		t.Error("nil must stay nil")
	}
	kolkata := time.FixedZone("IST", 5*3600+30*60)
	value := time.Date(2026, 9, 2, 10, 30, 0, 0, kolkata)
	got := normalizeOptionalTimestamp(&value)
	if got == nil {
		t.Fatal("a non-nil value must survive")
	}
	if got.UnixMilli() != 1788325200000 {
		t.Errorf("epoch_ms %d, want 1788325200000 -- the wrapper must convert too",
			got.UnixMilli())
	}
	// The caller's value must not be mutated in place.
	if value.Location() == time.UTC {
		t.Error("normalizeOptionalTimestamp mutated its argument")
	}
}
