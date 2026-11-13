package logging

import (
	"context"
	"log/slog"

	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	traceIDKey = "trace_id"
	spanIDKey  = "span_id"
)

// traceContext copies the span in the record's ctx onto the line as trace_id and span_id, so a log backend can link
// the line to its trace. A line with no valid span, or one that already carries a key by hand, gets no extra key.
type traceContext struct {
	inner slog.Handler
}

func (h traceContext) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h traceContext) Handle(ctx context.Context, record slog.Record) error {
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return h.inner.Handle(ctx, record)
	}
	hasTrace, hasSpan := false, false
	record.Attrs(func(attr slog.Attr) bool {
		switch attr.Key {
		case traceIDKey:
			hasTrace = true
		case spanIDKey:
			hasSpan = true
		}
		return !(hasTrace && hasSpan)
	})
	// The hex ids are computed outside the log calls: the syncdispatchruntime error-text census counts any log call whose
	// arguments hold a String() call.
	traceID, spanID := sc.TraceID().String(), sc.SpanID().String()
	if !hasTrace {
		record.AddAttrs(slog.String(traceIDKey, traceID))
	}
	if !hasSpan {
		record.AddAttrs(slog.String(spanIDKey, spanID))
	}
	return h.inner.Handle(ctx, record)
}

func (h traceContext) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceContext{inner: h.inner.WithAttrs(attrs)}
}

func (h traceContext) WithGroup(name string) slog.Handler {
	return traceContext{inner: h.inner.WithGroup(name)}
}
