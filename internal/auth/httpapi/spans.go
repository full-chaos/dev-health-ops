package httpapi

import (
	"context"
	"net/http"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ProbePaths are the exact request paths every listener of this package
// answers for a liveness or readiness probe (the kubelet and the ingress hit
// them every few seconds). They are never traced: the python api's
// FastAPIInstrumentor excludes them too, and a span per probe is volume with no
// information. The routes register through this table (internal/api/health),
// and a test walks every listener's registered routes so a new probe route
// that is missing here fails the build.
var ProbePaths = []string{"/health", "/ready", "/health/workers"}

func isProbePath(path string) bool {
	return pathIn(ProbePaths, path)
}

func pathIn(paths []string, path string) bool {
	for _, probe := range paths {
		if path == probe {
			return true
		}
	}
	return false
}

const (
	spanTracerName = "github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	// listenerAttribute is namespaced: it is this repo's attribute, not an
	// OpenTelemetry semantic-convention one.
	listenerAttribute = "dev_health.listener"
)

// spanObserver starts one server span per non-probe request and ends it with
// the registered route pattern the access observer learned. The span name and
// route are the registered pattern or UnmatchedRoute, never the raw path; no
// query string, header, token, body, client address, user or org value is an
// attribute.
type spanObserver struct {
	listener string
	// trustRemoteSampling extracts and honours an incoming traceparent (the
	// internal listener: its callers are in-cluster). Off, the incoming
	// traceparent is ignored entirely and every request starts a new root span
	// with a locally made trace id, decided by the root sampler: a caller on the
	// public or billing-edge listener chooses neither the trace id nor the
	// sampling decision, so it can neither force recording nor write spans into
	// a trace it names.
	trustRemoteSampling bool
	// probes are the exact paths this observer never traces. Nil means
	// ProbePaths (the api listeners').
	probes []string
}

func (s spanObserver) isProbe(path string) bool {
	if s.probes != nil {
		return pathIn(s.probes, path)
	}
	return isProbePath(path)
}

// start returns the request context to serve with and the span, or r's own
// context and nil for a probe path.
func (s spanObserver) start(r *http.Request) (context.Context, trace.Span) {
	ctx := r.Context()
	if s.isProbe(r.URL.Path) {
		return ctx, nil
	}
	options := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
	}
	if s.trustRemoteSampling {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
		// The caller's tracestate is a request header value: a span inherits
		// and exports its parent's, so it is dropped. Trace id, parent span id
		// and the sampled flag are what the in-cluster caller sends to be used.
		if remote := trace.SpanContextFromContext(ctx); remote.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, remote.WithTraceState(trace.TraceState{}))
		}
	} else {
		options = append(options, trace.WithNewRoot())
	}
	// The box a handler records its 404 cause into (RecordNotFoundCause); read
	// once, by finish.
	ctx = context.WithValue(ctx, notFoundCauseKey{}, &notFoundCauseBox{})
	method := boundedMethod(r.Method)
	options = append(options, trace.WithAttributes(
		attribute.String("http.request.method", method),
		attribute.String(listenerAttribute, s.listener),
	))
	return otel.Tracer(spanTracerName).Start(ctx, method+" "+UnmatchedRoute, options...)
}

// finish names and ends the span. aborted is true when the handler did not
// return (http.ErrAbortHandler re-panicked to net/http): the response was cut
// off, whatever status had been committed before, so the span is an error one.
// status is 0 when none was committed.
func (s spanObserver) finish(ctx context.Context, span trace.Span, method, pattern string, status int, aborted bool) {
	if span == nil {
		return
	}
	matched := pattern != ""
	if !matched {
		pattern = UnmatchedRoute
	} else {
		span.SetAttributes(attribute.String("http.route", pattern))
	}
	span.SetName(method + " " + pattern)
	if status > 0 {
		span.SetAttributes(attribute.Int("http.response.status_code", status))
	}
	if status == http.StatusNotFound {
		span.SetAttributes(attribute.String(NotFoundCauseAttribute, string(notFoundCause(ctx, matched))))
	}
	switch {
	case aborted:
		span.SetStatus(codes.Error, "aborted")
	case status >= 500:
		span.SetStatus(codes.Error, "")
	}
	span.End()
}

func boundedMethod(method string) string {
	if !knownMethods[method] {
		return "OTHER"
	}
	return method
}

// GraphQLErrorCountAttribute is the span attribute that says how many errors a
// GraphQL response carried. gqlgen answers a resolver error with HTTP 200, so
// the status code alone cannot show an error share; the count is a number only,
// never an error message or a variable value.
const GraphQLErrorCountAttribute = "dev_health.graphql.error_count"

// RecordGraphQLErrorCount sets GraphQLErrorCountAttribute on the request's
// server span (the span in ctx); a no-op without a recording span.
func RecordGraphQLErrorCount(ctx context.Context, count int) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.Int(GraphQLErrorCountAttribute, count))
}

// NotFoundCause is why a request was answered 404: a CLOSED enum, never a
// document, digest, path or org value.
type NotFoundCause string

const (
	// NotFoundUnregisteredDocument: a GraphQL document whose digest is not
	// registered (the safe default: it is not served by Go).
	NotFoundUnregisteredDocument NotFoundCause = "unregistered_document"
	// NotFoundIDEOff: a browser GET to /graphql while the GraphQL IDE is off.
	NotFoundIDEOff NotFoundCause = "ide_off"
	// NotFoundNoRoute: no route matched (the mux's own 404), or a catch-all that
	// answers "does not exist".
	NotFoundNoRoute NotFoundCause = "not_found"
	// NotFoundOther: a 404 whose handler recorded no cause, or an unknown one.
	NotFoundOther NotFoundCause = "other"
)

// NotFoundCauses is the closed list, for tests and docs.
var NotFoundCauses = []NotFoundCause{NotFoundUnregisteredDocument, NotFoundIDEOff, NotFoundNoRoute, NotFoundOther}

// NotFoundCauseAttribute is the server-span attribute that says why a 404 was
// answered; it is set only on a span whose status is 404.
const NotFoundCauseAttribute = "dev_health.http.not_found_cause"

type notFoundCauseKey struct{}

type notFoundCauseBox struct{ cause atomic.Value }

// RecordNotFoundCause records why the handler is about to answer 404. The last
// call wins. A value outside the closed list is recorded as NotFoundOther, so
// the attribute can never carry free text. A no-op on a context without a span
// observer (a probe path, an untraced listener).
func RecordNotFoundCause(ctx context.Context, cause NotFoundCause) {
	box, ok := ctx.Value(notFoundCauseKey{}).(*notFoundCauseBox)
	if !ok {
		return
	}
	for _, known := range NotFoundCauses {
		if cause == known {
			box.cause.Store(cause)
			return
		}
	}
	box.cause.Store(NotFoundOther)
}

// notFoundCause is the cause finish stamps on a 404 span: what the handler
// recorded, else NotFoundNoRoute when no route matched, else NotFoundOther.
func notFoundCause(ctx context.Context, matched bool) NotFoundCause {
	if box, ok := ctx.Value(notFoundCauseKey{}).(*notFoundCauseBox); ok {
		if cause, ok := box.cause.Load().(NotFoundCause); ok {
			return cause
		}
	}
	if !matched {
		return NotFoundNoRoute
	}
	return NotFoundOther
}
