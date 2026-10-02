package logging

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
)

// ErrorSpanAttributes is ErrorAttrs for a span event or span: `error.class`, `error.type` and, for a PostgreSQL error,
// `error.code` (the SQLSTATE). Identity-derived only: no text of the error (CHAOS-7936, same rule as ErrorAttrs).
func ErrorSpanAttributes(err error) []attribute.KeyValue {
	attrs := ErrorAttrs(err)
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		key := "error." + strings.TrimPrefix(attr.Key, "error_")
		out = append(out, attribute.String(key, attr.Value.String()))
	}
	return out
}
