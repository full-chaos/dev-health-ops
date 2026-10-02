package tracing

import (
	"context"
	"errors"
	"fmt"

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

// EndWorkSpan ends span. A non-nil err adds an "exception" event carrying only
// exception.type (the Go type of the innermost wrapped error, a bounded fixed
// word) and sets the span status to Error with no description, so a failed unit
// of work is separable from a quiet one. The error TEXT is deliberately never
// recorded: stream handlers and sinks wrap driver errors that can echo payload
// fragments (the process log already carries the full chain, server-side). A
// nil err leaves the status unset. A context.Canceled err is a
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
		span.AddEvent("exception", oteltrace.WithAttributes(attribute.String("exception.type", errorClass(err))))
		span.SetStatus(codes.Error, "")
	}
	span.End()
}

// errorClass is the Go type name of the innermost error in err's Unwrap chain.
func errorClass(err error) string {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return fmt.Sprintf("%T", err)
		}
		err = next
	}
}
