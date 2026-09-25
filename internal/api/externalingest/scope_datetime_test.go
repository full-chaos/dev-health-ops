package externalingest

import (
	"errors"
	"testing"
)

// TestScopeDatetimeRoundTripsTheFromISOFormatForms pins the stored scope
// datetime's answer for forms datetime.fromisoformat reads beyond the extended
// ones, through status.py's `_parse_dt_required` (a naive value is UTC): a
// microsecond offset keeps the wall clock, and a value that does not parse
// raises (ErrStoredJSONColumn), it is not null.
func TestScopeDatetimeRoundTripsTheFromISOFormatForms(t *testing.T) {
	for input, want := range map[string]any{
		"2026-01-01T00:00:00+00:00:00.000001": "2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00-00:00:00.000001": "2026-01-01T00:00:00Z",
		"20260101T013045Z":                    "2026-01-01T01:30:45Z",
		"2026-W01-1":                          "2025-12-29T00:00:00Z",
		"2026-01-01T24:00:00+05:30":           "2026-01-02T00:00:00+05:30",
		"2026-01-01T12:00:00.5-03:00":         "2026-01-01T12:00:00.500000-03:00",
		"20260101":                            "2026-01-01T00:00:00Z",
	} {
		got, err := scopeDatetime(scopeOf(t, `{"windowStartedAt":"`+input+`"}`), "windowStartedAt")
		if err != nil || got != want {
			t.Errorf("scopeDatetime(%q) = (%v, %v), want %v", input, got, err, want)
		}
	}
	if _, err := scopeDatetime(scopeOf(t, `{"windowStartedAt":"2026-01-01T"}`), "windowStartedAt"); !errors.Is(err, ErrStoredJSONColumn) {
		t.Errorf("an unparseable datetime raises: got %v", err)
	}
}
