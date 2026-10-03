// Package errortext is the one sanitizer for error text that is persisted or
// logged: credential-shaped substrings are replaced and the text is bounded. It
// is a leaf package (stdlib only) so that every layer, including the job runtime,
// can import it without a cycle (CHAOS-7943). It is the ONE implementation: the Python-parity
// matchers (matchers.go) answer every caller (CHAOS-7947). It imports the logging package for the credential-shape pass (stdlib
// plus the secrets leaf below it; no import cycle with the job runtime).
package errortext

import "github.com/full-chaos/dev-health-ops/internal/platform/logging"

const redactionMarker = "[REDACTED]"

// defaultMaxErrorTextLength mirrors error_sanitize.py's DEFAULT_MAX_ERROR_TEXT_LENGTH.
const defaultMaxErrorTextLength = 4000

const truncationSuffix = "...[truncated]"

// Redact is error_sanitize.py's pattern pass: every credential-shaped substring is replaced by "[REDACTED]", in the Python
// file's own order, with no cap. Empty input is returned as is.
func Redact(text string) string {
	if text == "" {
		return text
	}
	runes := []rune(text)
	for _, match := range matchers {
		runes = substitute(runes, match)
	}
	return string(runes)
}

// Truncate caps text at maxLength code points with "...[truncated]" (a maxLength of 0 or less means no cap, as Python's
// max_length=None); by rune, so a multi-byte rune is never split.
func Truncate(text string, maxLength int) string {
	if maxLength <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxLength {
		return text
	}
	suffix := []rune(truncationSuffix)
	if maxLength > len(suffix) {
		return string(runes[:maxLength-len(suffix)]) + truncationSuffix
	}
	return string(runes[:maxLength])
}

// Sanitize is sanitize_error_text for the STRING-input path with Python's default cap of 4000 (the exception-object branch
// has no Go counterpart). Empty input is returned as is.
func Sanitize(text string) string {
	return Truncate(Redact(text), defaultMaxErrorTextLength)
}

// SanitizeHardened is the ONE composition every caller that stores, logs or returns error text uses (CHAOS-7947): the
// Python-parity pattern pass first (so the recorded Python answer is always applied), then the credential shapes the Python
// list never had (LLM-provider, Stripe, Google, Slack and JWT keys by prefix, and a long value behind a credential word,
// CHAOS-7937), then the cap, so a cap can never cut a key to a fragment below a shape's minimum length. maxLength <= 0 = no cap.
// It differs from Sanitize only on text holding such a shape (a hardening, not a parity break).
func SanitizeHardened(text string, maxLength int) string {
	if text == "" {
		return text
	}
	return Truncate(logging.RedactCredentialShapes(Redact(text)), maxLength)
}

// SanitizeHardenedDefault is SanitizeHardened with Python's default cap of 4000.
func SanitizeHardenedDefault(text string) string {
	return SanitizeHardened(text, defaultMaxErrorTextLength)
}
