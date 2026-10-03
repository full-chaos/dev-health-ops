package syncdispatchruntime

import "github.com/full-chaos/dev-health-ops/internal/platform/errortext"

// sanitizeErrorText is internal/platform/errortext.SanitizeHardenedShapesFirstDefault: the ONE sanitizer engine (CHAOS-7947) in
// the order this path always ran (credential shapes first); see that function for why the order stays.
func sanitizeErrorText(text string) string { return errortext.SanitizeHardenedShapesFirstDefault(text) }

// SanitizeErrorText is sanitizeErrorText for callers outside the package that log or store error text
// (CHAOS-7132): credential-shaped substrings are replaced and the text is bounded.
func SanitizeErrorText(text string) string { return sanitizeErrorText(text) }
