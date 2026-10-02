package syncdispatchruntime

import (
	"strings"
	"testing"
)

// TestSanitizeErrorTextWrappersReachTheLeafSanitizer pins CHAOS-7943: both
// package-level entry points (the unexported one the writers call and the
// exported one other packages call) redact through internal/platform/errortext.
func TestSanitizeErrorTextWrappersReachTheLeafSanitizer(t *testing.T) {
	const secret = "ghp_abcdefghijklmnopqrstuvwx"
	for name, fn := range map[string]func(string) string{"sanitizeErrorText": sanitizeErrorText, "SanitizeErrorText": SanitizeErrorText} {
		got := fn("push failed: " + secret)
		if strings.Contains(got, secret) || !strings.Contains(got, "[REDACTED]") {
			t.Errorf("%s did not redact: %q", name, got)
		}
	}
}
