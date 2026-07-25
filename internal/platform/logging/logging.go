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

// RedactText removes supported DSNs and URLs containing userinfo from free-form
// errors before they can reach operator logs.
func RedactText(value string) string {
	value = dsnPattern.ReplaceAllString(value, redacted)
	return credentialURLPattern.ReplaceAllString(value, redacted)
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

func sensitiveKey(key string) bool {
	key = strings.ToLower(key)
	for _, marker := range []string{
		"authorization",
		"cookie",
		"credential",
		"database_url",
		"dsn",
		"password",
		"passwd",
		"secret",
		"token",
		"uri",
	} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}
