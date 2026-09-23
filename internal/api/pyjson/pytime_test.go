package pyjson

import (
	"testing"
	"time"
)

// TestTimeStringMatchesPydantic pins the exact strings a live pydantic
// v2 model_dump_json() produced for the same instants (see pytime.go's
// doc comment) -- every microsecond boundary that behaves differently
// under Go's own RFC3339Nano trimming.
func TestTimeStringMatchesPydantic(t *testing.T) {
	cases := []struct {
		microseconds int
		want         string
	}{
		{0, "2026-09-23T12:00:00Z"},
		{1, "2026-09-23T12:00:00.000001Z"},
		{100, "2026-09-23T12:00:00.000100Z"},
		{100000, "2026-09-23T12:00:00.100000Z"},
		{999999, "2026-09-23T12:00:00.999999Z"},
		{123456, "2026-09-23T12:00:00.123456Z"},
	}
	for _, tc := range cases {
		instant := time.Date(2026, 9, 23, 12, 0, 0, tc.microseconds*1000, time.UTC)
		if got := TimeString(instant); got != tc.want {
			t.Fatalf("TimeString(microseconds=%d) = %q, want %q", tc.microseconds, got, tc.want)
		}
	}
}

// TestTimeStringConvertsToUTC proves a non-UTC input is normalized first --
// this api never stores or returns anything but UTC, but a caller building
// a value by hand (a test, a bug) must not silently leak a local offset
// into the response.
func TestTimeStringConvertsToUTC(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	instant := time.Date(2026, 9, 23, 7, 0, 0, 0, loc) // 12:00:00 UTC
	if got, want := TimeString(instant), "2026-09-23T12:00:00Z"; got != want {
		t.Fatalf("TimeString(non-UTC) = %q, want %q", got, want)
	}
}
