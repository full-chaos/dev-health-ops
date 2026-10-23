package apiservice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func wideTrustLog(t *testing.T, trusted, fenced string) string {
	t.Helper()
	t.Setenv("TRUSTED_PROXIES", trusted)
	t.Setenv("TRUSTED_PROXIES_NETWORK_FENCED", fenced)
	var out bytes.Buffer
	logWideTrust(slog.New(slog.NewTextHandler(&out, nil)))
	return out.String()
}

func TestWideTrustIsAWarningWithoutTheFenceMarker(t *testing.T) {
	for _, marker := range []string{"", "false", "TRUE", "1", "yes"} {
		got := wideTrustLog(t, "10.42.0.0/24", marker)
		if !strings.Contains(got, "level=WARN") || strings.Contains(got, "fenced") {
			t.Errorf("marker %q: %q, want a warning that does not claim a fence", marker, got)
		}
	}
}

func TestWideTrustIsInformationalOnlyWithTheFenceMarker(t *testing.T) {
	got := wideTrustLog(t, "10.42.0.0/24", "true")
	if !strings.Contains(got, "level=INFO") || strings.Contains(got, "level=WARN") || !strings.Contains(got, "count=1") {
		t.Errorf("log = %q, want one informational line with the count", got)
	}
}

func TestNarrowTrustLogsNothingWhateverTheMarker(t *testing.T) {
	for _, marker := range []string{"", "true"} {
		if got := wideTrustLog(t, "10.42.0.6,10.42.0.0/28", marker); got != "" {
			t.Errorf("marker %q: log = %q, want nothing", marker, got)
		}
	}
}
