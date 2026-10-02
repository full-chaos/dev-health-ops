package syncdispatchruntime

import "github.com/full-chaos/dev-health-ops/internal/platform/errortext"

// sanitizeErrorText is internal/platform/errortext.Sanitize (CHAOS-7943: the
// sanitizer moved to a leaf package; this wrapper keeps the callers unchanged).
func sanitizeErrorText(text string) string { return errortext.Sanitize(text) }

// SanitizeErrorText is sanitizeErrorText for callers outside the package that log or store error text
// (CHAOS-7132): credential-shaped substrings are replaced and the text is bounded.
func SanitizeErrorText(text string) string { return sanitizeErrorText(text) }
