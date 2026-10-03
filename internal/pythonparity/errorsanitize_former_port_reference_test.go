package pythonparity_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// formerSecretPatterns is the sync writers' former RE2 sanitizer (internal/platform/errortext at beec298d34), kept here as a
// TEST-ONLY reference and deleted from production (CHAOS-7947: one sanitizer). It is the oracle of the monotone gate (D4495).
var formerSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(authorization|proxy-authorization)\s*[:=]\s*\S+(?:\s+\S+)?`),
	regexp.MustCompile(`(?i)\bbearer\s+\S+`),
	regexp.MustCompile(`(?i)\bbasic\s+[a-z0-9+/=]{8,}\b`),
	regexp.MustCompile(`(?i)\bghp_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bgho_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghu_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghs_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghr_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bgithub_pat_[a-z0-9_]{20,}\b`),
	regexp.MustCompile(`(?i)\bglpat-[a-z0-9_-]{20,}\b`),
	regexp.MustCompile(`(?i)\bxox[baprs]-[a-z0-9-]{10,}\b`),
	regexp.MustCompile(`(?i)\b(private_token|access_token|api_key|apikey|client_secret|secret|token)\s*[:=]\s*\S+`),
	regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s/@]+@`),
}

// formerSyncSanitize is what syncdispatchruntime.SanitizeErrorText was on main: the shape pass, then the RE2 patterns, then the cap.
func formerSyncSanitize(text string) string {
	if text == "" {
		return text
	}
	sanitized := logging.RedactCredentialShapes(text)
	for _, pattern := range formerSecretPatterns {
		sanitized = pattern.ReplaceAllString(sanitized, "[REDACTED]")
	}
	runes := []rune(sanitized)
	if len(runes) > 4000 {
		sanitized = string(runes[:4000-len("...[truncated]")]) + "...[truncated]"
	}
	return sanitized
}

var visibleTokenPattern = regexp.MustCompile(`[A-Za-z0-9_\x{80}-\x{10FFFF}]{4,}`)

// TestSyncEntryPointNeverHidesLessThanTheFormerPort is the monotone gate (D4495) for the sync writers' path: for every corpus
// text, no token of 4 or more characters that the former port hid (absent from its output) is visible in the tip's output.
// RED on a composition that runs the Python-parity pass before the shapes, or without the ASCII pass.
func TestSyncEntryPointNeverHidesLessThanTheFormerPort(t *testing.T) {
	compared, shown := 0, 0
	for _, pair := range sanitizeCorpus() {
		text, cap := pair[0].(string), pair[1].(int)
		if cap != 0 {
			continue
		}
		compared++
		former, tip := formerSyncSanitize(text), syncdispatchruntime.SanitizeErrorText(text)
		for _, token := range visibleTokenPattern.FindAllString(text, -1) {
			if !strings.Contains(former, token) && strings.Contains(tip, token) {
				shown++
				if shown <= 10 {
					t.Errorf("the tip shows %q that the former port hid: %q\n former %q\n tip    %q", token, text, former, tip)
				}
				break
			}
		}
	}
	if compared == 0 {
		t.Fatal("no text compared")
	}
	t.Logf("%d texts compared, %d where the tip shows what the former port hid", compared, shown)
}
