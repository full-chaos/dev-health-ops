package syncdispatchruntime

import (
	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// sanitizeErrorText is internal/platform/errortext.Sanitize (CHAOS-7943: the sanitizer moved to a leaf package; this wrapper
// keeps the callers unchanged) preceded by the credential-shape pass the log redactor uses (CHAOS-7937): LLM-provider, Stripe,
// Google, Slack and JWT keys by prefix, and a long value behind a credential word. The exact sanitizer stays what it was; the
// hardening is added here and in pythonparity.SanitizeErrorTextHardened (a named difference: Go redacts MORE than the recorded
// Python answer at these callers).
func sanitizeErrorText(text string) string {
	// the shape pass runs BEFORE the exact sanitizer: its 4000-rune cap would otherwise cut a key to a fragment below the shape's
	// minimum length, which no later pass can recognise
	// the userinfo pass runs LAST (after the cap): a pass in front of the exact sanitizer would change what it hides
	// the pass can add the marker, so the text is cut again with the same cap
	return errortext.Cap(logging.RedactUserinfoLast(errortext.Sanitize(logging.RedactCredentialShapesNoUserinfo(text))))
}

// SanitizeErrorText is sanitizeErrorText for callers outside the package that log or store error text
// (CHAOS-7132): credential-shaped substrings are replaced and the text is bounded.
func SanitizeErrorText(text string) string { return sanitizeErrorText(text) }
