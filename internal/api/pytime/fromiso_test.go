package pytime

import (
	"testing"
	"time"
)

// TestFromISOFormatForms pins each form Python 3.14's
// datetime.fromisoformat reads, and near misses it refuses, without a
// Python process (the live oracle runs the full corpus).
func TestFromISOFormatForms(t *testing.T) {
	utc := func(y int, m time.Month, d, h, mi, s, us int) time.Time {
		return time.Date(y, m, d, h, mi, s, us*1000, time.UTC)
	}
	for _, tc := range []struct {
		text             string
		wall             time.Time
		aware            bool
		offset, offsetUS int
	}{
		{"2026-01-01", utc(2026, 1, 1, 0, 0, 0, 0), false, 0, 0},
		{"20260101", utc(2026, 1, 1, 0, 0, 0, 0), false, 0, 0},
		{"2026-W01-1", utc(2025, 12, 29, 0, 0, 0, 0), false, 0, 0},
		{"2026W014", utc(2026, 1, 1, 0, 0, 0, 0), false, 0, 0},
		{"2020-W53-7", utc(2021, 1, 3, 0, 0, 0, 0), false, 0, 0},
		{"20260101T013045.5Z", utc(2026, 1, 1, 1, 30, 45, 500000), true, 0, 0},
		{"2026-01-01T24:00:00", utc(2026, 1, 2, 0, 0, 0, 0), false, 0, 0},
		{"2026-12-31T24:00", utc(2027, 1, 1, 0, 0, 0, 0), false, 0, 0},
		{"2026-01-01é01:00", utc(2026, 1, 1, 1, 0, 0, 0), false, 0, 0},
		{"2026-01-01T00:00x+01:00", utc(2026, 1, 1, 0, 0, 0, 0), true, 3600, 0},
		{"2026-01-01T00:00:00-05:30:15.5", utc(2026, 1, 1, 0, 0, 0, 0), true, -19815, -500000},
		{"2026-01-01T00:00:00.1234567", utc(2026, 1, 1, 0, 0, 0, 123456), false, 0, 0},
	} {
		got, ok := FromISOFormat(tc.text)
		if !ok {
			t.Errorf("%q refused", tc.text)
			continue
		}
		wall := got.Time.Add(time.Duration(got.Offset)*time.Second + time.Duration(got.OffsetMicro)*time.Microsecond)
		if !wall.Equal(tc.wall) || got.Aware != tc.aware || got.Offset != tc.offset || got.OffsetMicro != tc.offsetUS {
			t.Errorf("%q = %v aware=%v offset=%d %d, want %v %v %d %d", tc.text, wall, got.Aware, got.Offset, got.OffsetMicro,
				tc.wall, tc.aware, tc.offset, tc.offsetUS)
		}
	}
	for _, text := range []string{
		"2026-01-01T", "2026-01-01 ", "2026-W011", "2026-01-01T24:00:01", "2026-02-29", "2026-01-01T00:00:00.+01:00",
		"2026-01-01T00:00:00+24:00", "2026-01-01T00:00x", "2026-01-01T0Z", "2026-01-01T00:00:00+05:30Z", "2026é01-01",
		"0000-01-01", "2026-13-01", "202601-01", "2025-W53-1",
	} {
		if got, ok := FromISOFormat(text); ok {
			t.Errorf("%q accepted as %v", text, got)
		}
	}
}

// TestABasicISODateIsNeverAUnixTimestamp pins why callers that follow
// datetime.fromisoformat with pydantic's lax parse (a backfill window
// boundary) must ask FromISOFormat first: pydantic reads the same digits
// as Unix seconds.
func TestABasicISODateIsNeverAUnixTimestamp(t *testing.T) {
	iso, ok := FromISOFormat("20260101")
	if !ok || iso.Time.Year() != 2026 {
		t.Fatalf("FromISOFormat(20260101) = %v %v, want 2026-01-01", iso, ok)
	}
	lax, failure := ParseDatetime("20260101")
	if failure != nil || lax.Time.Year() != 1970 {
		t.Fatalf("pydantic's parse of 20260101 = %v %v; the ordering pin assumes it reads Unix seconds", lax, failure)
	}
}

// TestPydanticWritesOffsetsAsPydanticCore pins pydantic-core's offset form:
// the offset rounded to whole seconds (half away from zero), Z when that is
// zero, else the sign and whole hours:minutes.
func TestPydanticWritesOffsetsAsPydanticCore(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		offset, offsetMicro int
		want                string
	}{
		{0, 0, "Z"}, {0, 499999, "Z"}, {0, -499999, "Z"}, {0, 500000, "+00:00"}, {0, -500000, "-00:00"},
		{59, 0, "+00:00"}, {-59, 0, "-00:00"}, {59, 500000, "+00:01"}, {-59, -500000, "-00:01"},
		{19815, 0, "+05:30"}, {-19815, -500000, "-05:30"}, {86399, 500000, "+24:00"}, {86399, 499999, "+23:59"},
		{3600, 0, "+01:00"}, {-3600, 0, "-01:00"},
	} {
		value := DateTime{Time: at.Add(-time.Duration(tc.offset)*time.Second - time.Duration(tc.offsetMicro)*time.Microsecond),
			Aware: true, Offset: tc.offset, OffsetMicro: tc.offsetMicro}
		if got := Pydantic(value); got != "2026-01-01T00:00:00"+tc.want {
			t.Errorf("offset %ds %dus: %s, want %s", tc.offset, tc.offsetMicro, got, "2026-01-01T00:00:00"+tc.want)
		}
	}
	if got := Pydantic(DateTime{Time: at}); got != "2026-01-01T00:00:00" {
		t.Errorf("naive: %s", got)
	}
}
