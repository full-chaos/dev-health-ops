package externalingest

import "testing"

// TestScopeDatetimeValueRoundTripsTheFromISOFormatForms pins the stored
// scope datetime's answer for forms datetime.fromisoformat reads beyond the
// extended ones, each against the Python api's value
// (TypeAdapter(datetime).dump_json(datetime.fromisoformat(s)), CPython
// 3.14.7, pydantic 2.13.4): a microsecond offset keeps the wall clock.
func TestScopeDatetimeValueRoundTripsTheFromISOFormatForms(t *testing.T) {
	for input, want := range map[string]any{
		"2026-01-01T00:00:00+00:00:00.000001": "2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00-00:00:00.000001": "2026-01-01T00:00:00Z",
		"20260101T013045Z":                    "2026-01-01T01:30:45Z",
		"2026-W01-1":                          "2025-12-29T00:00:00",
		"2026-01-01T24:00:00+05:30":           "2026-01-02T00:00:00+05:30",
		"2026-01-01T12:00:00.5-03:00":         "2026-01-01T12:00:00.500000-03:00",
		"20260101":                            "2026-01-01T00:00:00",
		"2026-01-01T":                         nil,
	} {
		if got := scopeDatetimeValue(input); got != want {
			t.Errorf("scopeDatetimeValue(%q) = %v, want %v", input, got, want)
		}
	}
}
