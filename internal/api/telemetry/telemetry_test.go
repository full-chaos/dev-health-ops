package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestLastReportAtRoundTripsTheFromISOFormatForms pins get_last_report_at
// ("Z" -> "+00:00", then datetime.fromisoformat, then the pydantic JSON
// form) for forms beyond the extended ones, each against the Python api's
// value (CPython 3.14.7, pydantic 2.13.4); an unreadable value is None and
// logged.
func TestLastReportAtRoundTripsTheFromISOFormatForms(t *testing.T) {
	for input, want := range map[string]any{
		"2026-01-01T00:00:00+00:00:00.000001": "2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00-00:00:00.000001": "2026-01-01T00:00:00Z",
		"20260101T013045Z":                    "2026-01-01T01:30:45Z",
		"2026-W01-1":                          "2025-12-29T00:00:00",
		"2026-01-01T24:00:00+05:30":           "2026-01-02T00:00:00+05:30",
		"20260101":                            "2026-01-01T00:00:00",
	} {
		var logs bytes.Buffer
		h := handlers{logger: slog.New(slog.NewTextHandler(&logs, nil))}
		value := input
		if got := h.lastReportAt(context.Background(), &value, true, "org"); got != want || logs.Len() != 0 {
			t.Errorf("lastReportAt(%q) = %v (logs %q), want %v", input, got, logs.String(), want)
		}
	}
	var logs bytes.Buffer
	h := handlers{logger: slog.New(slog.NewTextHandler(&logs, nil))}
	bad := "2026-01-01T"
	if got := h.lastReportAt(context.Background(), &bad, true, "org"); got != nil || !strings.Contains(logs.String(), "invalid telemetry_last_report_at value") {
		t.Errorf("lastReportAt(%q) = %v, logs %q; want nil and a warning", bad, got, logs.String())
	}
}
