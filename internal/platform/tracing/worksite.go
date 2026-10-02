package tracing

import (
	"context"
	"encoding/json"
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

// StageAttribute names WHERE a unit of work failed (read, handle, handoff,
// reconcile, engine, schedule, ...): a fixed word set by the loop, so an Error
// span is actionable without the error text.
const StageAttribute = "dev_health.work.stage"

// Closed list of error classes a work span may carry (error.type). Anything
// not recognised is ErrorClassOther; the list is the whole vocabulary, so a
// span can never carry free text from an error.
const (
	ErrorClassTimeout = "timeout"
	ErrorClassDecode  = "decode"
	ErrorClassStore   = "store"
	ErrorClassOther   = "other"
)

// ErrorClasses is the closed list, for tests and docs.
var ErrorClasses = []string{ErrorClassTimeout, ErrorClassDecode, ErrorClassStore, ErrorClassOther}

// EndWorkSpan ends span. A non-nil err sets the span status to Error with the
// error CLASS as its description, adds an "exception" event carrying only
// error.type = that class, and nothing else: the error TEXT is deliberately
// never recorded, because stream handlers and sinks wrap driver errors that can
// echo an org marker, a stream key, a URL or payload fragments. The full chain
// stays in the process log line each loop already writes. A nil err leaves the
// status unset. A context.Canceled err is a shutdown, not a failure: it is
// marked dev_health.work.cancelled and left without an Error status. Safe with
// a nil span.
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
		class := ErrorClass(err)
		span.AddEvent("exception", oteltrace.WithAttributes(attribute.String("error.type", class)))
		span.SetStatus(codes.Error, class)
	}
	span.End()
}

// ErrorClass maps err to one of ErrorClasses by behaviour, never by text:
// a deadline or a net timeout is timeout; a JSON syntax/type error is decode;
// an error exposing a SQLSTATE (pgconn.PgError) is store; everything else is
// other.
func ErrorClass(err error) string {
	var netTimeout interface{ Timeout() bool }
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	var sqlState interface{ SQLState() string }
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netTimeout) && netTimeout.Timeout():
		return ErrorClassTimeout
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return ErrorClassDecode
	case errors.As(err, &sqlState):
		return ErrorClassStore
	default:
		return ErrorClassOther
	}
}
