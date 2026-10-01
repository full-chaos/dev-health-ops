package httpapi

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
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
	for _, probe := range ProbePaths {
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
	// trustRemoteSampling honours an incoming traceparent's sampled flag (the
	// internal listener: in-cluster callers). Off, the caller keeps its trace
	// id but the sampled flag is replaced by this process's own root-sampler
	// decision for that trace id, so a client on the public or billing-edge
	// listener cannot force recording.
	trustRemoteSampling bool
}

// start returns the request context to serve with and the span, or r's own
// context and nil for a probe path.
func (s spanObserver) start(r *http.Request) (context.Context, trace.Span) {
	ctx := r.Context()
	if isProbePath(r.URL.Path) {
		return ctx, nil
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
	if remote := trace.SpanContextFromContext(ctx); remote.IsValid() {
		if s.trustRemoteSampling {
			ctx = trace.ContextWithRemoteSpanContext(ctx, remote)
		} else {
			// Rebuilt from the ids and the local decision only: the caller's
			// tracestate is dropped with the caller's sampled flag.
			ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    remote.TraceID(),
				SpanID:     remote.SpanID(),
				TraceFlags: remote.TraceFlags().WithSampled(tracing.LocalSamplingDecision(remote.TraceID())),
				Remote:     true,
			}))
		}
	}
	method := boundedMethod(r.Method)
	return otel.Tracer(spanTracerName).Start(ctx, method+" "+UnmatchedRoute,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", method),
			attribute.String(listenerAttribute, s.listener),
		))
}

// finish names and ends the span. status is 0 for an aborted handler.
func (s spanObserver) finish(span trace.Span, method, pattern string, status int) {
	if span == nil {
		return
	}
	if pattern == "" {
		pattern = UnmatchedRoute
	} else {
		span.SetAttributes(attribute.String("http.route", pattern))
	}
	span.SetName(method + " " + pattern)
	switch {
	case status == 0:
		span.SetStatus(codes.Error, "aborted")
	case status >= 500:
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		span.SetStatus(codes.Error, "")
	default:
		span.SetAttributes(attribute.Int("http.response.status_code", status))
	}
	span.End()
}

func boundedMethod(method string) string {
	if !knownMethods[method] {
		return "OTHER"
	}
	return method
}
