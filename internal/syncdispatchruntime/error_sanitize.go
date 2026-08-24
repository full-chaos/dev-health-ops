package syncdispatchruntime

import "regexp"

// redactionMarker replaces any credential-shaped substring sanitizeErrorText
// recognizes. Mirrors src/dev_health_ops/sync/error_sanitize.py
// (REDACTION_MARKER) exactly -- tests assert this marker is present (and the
// original secret is not) rather than hardcoding the redaction text inline
// everywhere.
const redactionMarker = "[REDACTED]"

// defaultMaxErrorTextLength mirrors error_sanitize.py's
// DEFAULT_MAX_ERROR_TEXT_LENGTH. finalize_sync_run always calls
// sanitize_error_text with its default max_length, so this is the only cap
// this native port needs.
const defaultMaxErrorTextLength = 4000

const truncationSuffix = "...[truncated]"

// secretPatterns is a literal, order-preserving port of
// error_sanitize.py's _SECRET_PATTERNS tuple. Order matters: header-shaped
// matches must consume their whole "<Scheme> <credential>" pair before the
// narrower bare-token patterns get a chance to leave a dangling fragment --
// see the Python module's comment for the full rationale. Go's regexp
// (RE2) has no (?i) inline-flag scoping ambiguity here since each pattern
// opens with it, matching Python's per-pattern re.IGNORECASE-via-(?i) usage.
var secretPatterns = []*regexp.Regexp{
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

// sanitizeErrorText is the native port of error_sanitize.py's
// sanitize_error_text for the STRING-input path only. finalize_sync_run
// never calls it with an exception object (Go has no exception type to
// pass) -- it only ever sanitizes an already-stringified sync_runs.error /
// planner-recorded value, so the "exception with a class-name prefix"
// branch of the Python function has no native caller and is intentionally
// not ported. A nil/empty input maps 1:1 to Python's None/"" short-circuit:
// callers pass "" for a nil *string and get "" back, matching
// `sanitize_error_text(None) is None` / `sanitize_error_text("") == ""`
// closely enough that callers branch on emptiness the same way Python
// branches on `is None`.
func sanitizeErrorText(text string) string {
	if text == "" {
		return text
	}
	sanitized := text
	for _, pattern := range secretPatterns {
		sanitized = pattern.ReplaceAllString(sanitized, redactionMarker)
	}
	// Python's len()/slicing on str is by Unicode code point, not byte --
	// truncate by rune here too, both so the cap lines up with Python's for
	// non-ASCII text and so a multi-byte UTF-8 rune can never be split.
	runes := []rune(sanitized)
	if len(runes) > defaultMaxErrorTextLength {
		if defaultMaxErrorTextLength > len(truncationSuffix) {
			sanitized = string(runes[:defaultMaxErrorTextLength-len(truncationSuffix)]) + truncationSuffix
		} else {
			sanitized = string(runes[:defaultMaxErrorTextLength])
		}
	}
	return sanitized
}
