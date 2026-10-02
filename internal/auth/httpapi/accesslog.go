package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// UnmatchedRoute is the route label of a request no registered route took (an
// unknown path, or a method no route of the path allows on a catch-all). A raw
// path is never a label or a log field: it is unbounded and can carry ids.
const UnmatchedRoute = "unmatched"

const (
	requestsMetricName = "dev_health_api_http_requests_total"
	durationMetricName = "dev_health_api_http_request_duration_seconds"
	// invalidRequestID is what a log line carries for a client-supplied id
	// outside the narrow correlation charset.
	invalidRequestID = "invalid"
)

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// knownMethods bounds the method label: a client can send any token as the
// method, and each distinct one would otherwise be a new series.
var knownMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodDelete: true, http.MethodOptions: true,
	http.MethodConnect: true, http.MethodTrace: true,
}

type observedRouteKey struct{}

// observedRoute is filled by the route chain (or a 405) with the registered
// pattern that took the request; it stays empty for an unmatched one.
type observedRoute struct {
	pattern string
	// notFoundCause is what RecordNotFoundCause stored (an index; 0 = nothing).
	notFoundCause atomic.Uint32
}

// recordRoute notes the registered pattern that took r, for the access
// observer. It is a no-op for a server built without one.
func recordRoute(r *http.Request, pattern string) {
	if match, ok := r.Context().Value(observedRouteKey{}).(*observedRoute); ok {
		match.pattern = pattern
	}
}

// accessRecorder captures the status the response committed. It forwards
// Flush and unwraps for http.ResponseController, so a streaming handler is not
// blocked by the observer.
type accessRecorder struct {
	http.ResponseWriter
	status int
	// notFoundCause is what RecordNotFoundCause stored when this recorder is
	// the cause sink (TraceHandler); the access observer uses observedRoute.
	notFoundCause atomic.Uint32
}

func (a *accessRecorder) WriteHeader(status int) {
	if a.status == 0 && (status >= 200 || status == http.StatusSwitchingProtocols) {
		a.status = status
	}
	a.ResponseWriter.WriteHeader(status)
}

func (a *accessRecorder) Write(data []byte) (int, error) {
	if a.status == 0 {
		a.status = http.StatusOK
	}
	return a.ResponseWriter.Write(data)
}

func (a *accessRecorder) Flush() {
	if flusher, ok := a.ResponseWriter.(http.Flusher); ok {
		if a.status == 0 {
			a.status = http.StatusOK
		}
		flusher.Flush()
	}
}

func (a *accessRecorder) Unwrap() http.ResponseWriter { return a.ResponseWriter }

// accessObserver logs one structured line per request and records the
// per-route request counter and duration histogram. It carries no query string,
// header, token, body or client address: only the method, the registered route
// pattern, the status, the duration, the listener and the request id.
type accessObserver struct {
	logger   *slog.Logger
	listener string
	spans    spanObserver
	requests metric.Int64Counter
	duration metric.Float64Histogram
}

// newAccessObserver builds the instruments from the process's current meter
// provider. A failure to create one leaves that instrument off (the log line
// still flows): observability never stops the server.
func newAccessObserver(logger *slog.Logger, listener string, trustRemoteSampling bool) *accessObserver {
	observer := &accessObserver{logger: logger, listener: listener, spans: spanObserver{listener: listener, trustRemoteSampling: trustRemoteSampling}}
	meter := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/auth/httpapi")
	if counter, err := meter.Int64Counter(requestsMetricName,
		metric.WithDescription("HTTP requests served, by registered route pattern, method, status class and listener")); err == nil {
		observer.requests = counter
	} else {
		logger.Warn("access metrics: request counter unavailable", "error", err)
	}
	if histogram, err := meter.Float64Histogram(durationMetricName,
		metric.WithDescription("HTTP request duration in seconds, by registered route pattern, method, status class and listener"),
		metric.WithExplicitBucketBoundaries(durationBuckets...)); err == nil {
		observer.duration = histogram
	} else {
		logger.Warn("access metrics: duration histogram unavailable", "error", err)
	}
	return observer
}

func (o *accessObserver) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		match := &observedRoute{}
		recorder := &accessRecorder{ResponseWriter: w}
		spanCtx, span := o.spans.start(r)
		returned := false
		// The observation runs even when the handler aborts the response with
		// http.ErrAbortHandler (Recover re-panics it): an aborted request is
		// still a request that reached the route.
		defer func() {
			o.spans.finish(span, boundedMethod(r.Method), match.pattern, recorder.status, !returned, match.notFoundCause.Load())
			o.observe(r, match.pattern, recorder.status, time.Now().Sub(start))
		}()
		next.ServeHTTP(recorder, r.WithContext(context.WithValue(spanCtx, observedRouteKey{}, match)))
		returned = true
		// net/http answers 200 for a handler that returns without writing;
		// only an aborted handler (no return) leaves the status unset.
		if recorder.status == 0 {
			recorder.status = http.StatusOK
		}
	})
}

func (o *accessObserver) observe(r *http.Request, pattern string, status int, elapsed time.Duration) {
	if pattern == "" {
		pattern = UnmatchedRoute
	}
	method := r.Method
	if !knownMethods[method] {
		method = "OTHER"
	}
	class := statusClass(status)
	if elapsed < 0 {
		elapsed = 0
	}
	ctx := r.Context()
	attrs := metric.WithAttributes(
		attribute.String("route", pattern),
		attribute.String("method", method),
		attribute.String("status_class", class),
		attribute.String("listener", o.listener),
	)
	if o.requests != nil {
		o.requests.Add(ctx, 1, attrs)
	}
	if o.duration != nil {
		o.duration.Record(ctx, elapsed.Seconds(), attrs)
	}
	o.logger.LogAttrs(ctx, slog.LevelInfo, "http request",
		slog.String("method", method),
		slog.String("route", pattern),
		slog.Int("status", status),
		slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
		slog.String("listener", o.listener),
		slog.String("request_id", LoggableRequestID(ctx)),
	)
}

// statusClass is "2xx".."5xx"; a response that never committed a status (an
// aborted handler) or an out-of-range one is "none" / "other".
func statusClass(status int) string {
	switch {
	case status == 0:
		return "none"
	case status >= 100 && status < 600:
		return strconv.Itoa(status/100) + "xx"
	default:
		return "other"
	}
}

// LoggableRequestID is the request id bound to ctx as it may appear in a log
// line. The api echoes any acceptable X-Request-Id a client sends, so the
// bound value is client-controlled, and a credential (a JWT, an opaque token)
// can fit any charset rule. Only a canonical UUID (8-4-4-4-12 hex, which is
// every generated id) or a 32-hex id is logged; anything else is "invalid",
// and correlation for such ids is by the response header only.
func LoggableRequestID(ctx context.Context) string {
	id := RequestIDFrom(ctx)
	if id == "" {
		return ""
	}
	if !loggableRequestID(id) {
		return invalidRequestID
	}
	return id
}

func loggableRequestID(id string) bool {
	switch len(id) {
	case 32:
		return allHex(id)
	case 36:
		for i := 0; i < len(id); i++ {
			if i == 8 || i == 13 || i == 18 || i == 23 {
				if id[i] != '-' {
					return false
				}
			} else if !isHex(id[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func allHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isHex(s[i]) {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
