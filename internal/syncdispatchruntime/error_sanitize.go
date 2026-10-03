package syncdispatchruntime

import "github.com/full-chaos/dev-health-ops/internal/platform/errortext/hardened"

// sanitizeErrorText is internal/platform/errortext/hardened.SyncWriters: the ONE sanitizer engine (CHAOS-7947) in the order this
// path always ran (credential shapes first, the userinfo pass last); see that function for why the order stays.
func sanitizeErrorText(text string) string { return hardened.SyncWriters(text) }

// SanitizeErrorText is sanitizeErrorText for callers outside the package that log or store error text
// (CHAOS-7132): credential-shaped substrings are replaced and the text is bounded.
func SanitizeErrorText(text string) string { return sanitizeErrorText(text) }
