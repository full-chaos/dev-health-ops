package tracing

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// StartWorkSpan opens one span for a unit of worker-loop work (a scheduler
// tick, a stream batch or event). The tracer is looked up on every call, never
// cached: otel's global delegation binds a held Tracer to the first provider
// it sees, so a cached one would ignore a later provider (see
// internal/jobruntime/adapter.go jobTracerName).
//
// Attributes must be bounded and carry no payload, no customer object id and
// no credential: counts, fixed names and fixed outcome words only.
func StartWorkSpan(ctx context.Context, scope, name string, attributes ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	return otel.Tracer(scope).Start(ctx, name, oteltrace.WithAttributes(attributes...))
}

// EndWorkSpan ends span. A non-nil err is recorded as an exception event and
// sets the span status to Error, so a failed unit of work is separable from a
// quiet one; a nil err leaves the status unset. A context.Canceled err is a
// shutdown, not a failure: it is marked dev_health.work.cancelled and left
// without an Error status. Safe with a nil span.
func EndWorkSpan(span oteltrace.Span, err error) {
	if span == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		span.SetAttributes(attribute.Bool("dev_health.work.cancelled", true))
		span.End()
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "")
	}
	span.End()
}
