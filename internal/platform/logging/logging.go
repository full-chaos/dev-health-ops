// Package logging provides the process-wide structured logging policy.
package logging

import (
	"io"
	"log/slog"
	"regexp"
	"strings"
)

const redacted = "[REDACTED]"

var (
	dsnPattern           = regexp.MustCompile(`(?i)\b(?:postgres(?:ql)?|clickhouse|redis|rediss|valkey|https?)://[^\s"'<>]+`)
	credentialURLPattern = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^/@\s"'<>]+:[^@\s"'<>]+@[^\s"'<>]+`)
	// bareCredentialPatterns catch credential-shaped substrings OUTSIDE a
	// URL -- an HTTP request/response header, or a bare "key=value" pair --
	// that dsnPattern/credentialURLPattern's URL-anchored matching cannot
	// see. Added for CHAOS-4582 (codex review): a third-party response body
	// is untrusted content this process does not control the shape of, so
	// RedactText's coverage cannot stop at "looks like a URL". Mirrors
	// src/dev_health_ops/sync/error_sanitize.py's _SECRET_PATTERNS (order
	// matters there for the same reason: a header-shaped match must consume
	// its whole "<scheme> <credential>" pair before a narrower bare-token
	// pattern gets a chance to leave a dangling fragment).
	bareCredentialPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(authorization|proxy-authorization)\s*[:=]\s*\S+(?:\s+\S+)?`),
		regexp.MustCompile(`(?i)\bbearer\s+\S+`),
		regexp.MustCompile(`(?i)\bbasic\s+[a-z0-9+/=]{8,}\b`),
		regexp.MustCompile(`(?i)\b(private_token|access_token|api_key|apikey|client_secret|secret|token)\s*[:=]\s*\S+`),
	}
)

// NewJSON returns a logger with a redacting handler. Redaction at the handler
// boundary is defense in depth; callers should still only emit safe fields.
func NewJSON(output io.Writer, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: redactAttr,
	})
	return slog.New(handler)
}

// RedactText removes supported DSNs, URLs containing userinfo, and
// bare credential-shaped substrings (an Authorization header, a bearer
// token, a "token="/"secret=" pair) from free-form errors before they can
// reach operator logs.
func RedactText(value string) string {
	value = dsnPattern.ReplaceAllString(value, redacted)
	value = credentialURLPattern.ReplaceAllString(value, redacted)
	for _, pattern := range bareCredentialPatterns {
		value = pattern.ReplaceAllString(value, redacted)
	}
	return value
}

func redactAttr(_ []string, attr slog.Attr) slog.Attr {
	if sensitiveKey(attr.Key) {
		return slog.String(attr.Key, redacted)
	}

	value := attr.Value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(RedactText(value.String()))
	case slog.KindAny:
		if err, ok := value.Any().(error); ok {
			attr.Value = slog.StringValue(RedactText(err.Error()))
		}
	}
	return attr
}

// sensitiveKeySegments are markers matched against a whole `_`-separated
// segment of the key, never a raw substring -- a raw substring match makes
// "security" redact as a false positive of "uri" (sec-URI-ty). Matching on
// segments keeps every genuine case (database_uri, redis_uri, api_token,
// oauth_secret, db_password, dsn) while a key that merely contains one of
// these words as a fragment of a longer, unrelated segment no longer does.
var sensitiveKeySegments = map[string]bool{
	"authorization": true,
	"cookie":        true,
	"credential":    true,
	"dsn":           true,
	"password":      true,
	"passwd":        true,
	"secret":        true,
	"token":         true,
	"uri":           true,
}

// sensitiveKeySegmentPairs are markers that span two adjacent segments. A
// bare "url" segment is not sensitive by itself -- most *_url settings
// (webhook_url, avatar_url) name a public endpoint, not a credential -- so
// only the specific compound spellings known to carry a DSN are listed here.
var sensitiveKeySegmentPairs = [][2]string{
	{"database", "url"},
}

func sensitiveKey(key string) bool {
	segments := strings.Split(strings.ToLower(key), "_")
	for _, segment := range segments {
		if sensitiveKeySegments[segment] {
			return true
		}
	}
	for _, pair := range sensitiveKeySegmentPairs {
		for i := 0; i+1 < len(segments); i++ {
			if segments[i] == pair[0] && segments[i+1] == pair[1] {
				return true
			}
		}
	}
	return false
}
