package syncdispatchruntime

import "github.com/full-chaos/dev-health-ops/internal/platform/errortext"

// sanitizeErrorText is internal/platform/errortext.SanitizeHardenedDefault: the ONE sanitizer (CHAOS-7947), the same
// composition pythonparity.SanitizeErrorTextHardened uses (Python-parity patterns, then credential shapes, then the 4000 cap).
func sanitizeErrorText(text string) string { return errortext.SanitizeHardenedDefault(text) }

// SanitizeErrorText is sanitizeErrorText for callers outside the package that log or store error text
// (CHAOS-7132): credential-shaped substrings are replaced and the text is bounded.
func SanitizeErrorText(text string) string { return sanitizeErrorText(text) }
