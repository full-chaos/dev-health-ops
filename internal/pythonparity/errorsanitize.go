package pythonparity

import (
	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
)

// SanitizeErrorText is src/dev_health_ops/sync/error_sanitize.py's
// sanitize_error_text over a string (the exception-object branch has no Go
// counterpart): each credential-shaped pattern is replaced by "[REDACTED]" in
// the file's own order, then the text is capped at maxLength code points with
// "...[truncated]" (a maxLength of 0 or less means no cap, as Python's
// max_length=None). Empty input is returned as is.
//
// The matchers are internal/platform/errortext's (the ONE sanitizer, CHAOS-7947); the frozen Python oracle test in this package
// compares them with `re` over a corpus.
func SanitizeErrorText(text string, maxLength int) string {
	return sanitizeErrorText(text, maxLength, false)
}

// SanitizeErrorTextHardened is SanitizeErrorText plus the credential shapes Python's list never had: LLM-provider, Stripe,
// Google, Slack and JWT keys by prefix, and a long value behind a credential word (CHAOS-7937). The frozen Python oracle
// pins SanitizeErrorText exactly; every production caller that stores or returns error text uses this one, so the two differ
// only on text that holds such a shape (a named difference: a hardening, not a parity break).
func SanitizeErrorTextHardened(text string, maxLength int) string {
	return sanitizeErrorText(text, maxLength, true)
}

func sanitizeErrorText(text string, maxLength int, harden bool) string {
	if harden {
		return errortext.SanitizeHardened(text, maxLength)
	}
	return errortext.Truncate(errortext.Redact(text), maxLength)
}
