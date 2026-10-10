package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// TraceOptions configures TraceHandler.
type TraceOptions struct {
	// Listener is the dev_health.listener attribute ("public", "internal", ...).
	Listener string
	// TrustRemoteSampling extracts and honours an incoming traceparent (a
	// listener whose callers are in-cluster). Off, the incoming traceparent is
	// ignored and every request starts a new root span.
	TrustRemoteSampling bool
	// ProbePaths are the exact paths that get no span and no request line. Nil means ProbePaths.
	ProbePaths []string
	// Logger writes the one request line per request. Nil means slog.Default(),
	// which a binary started through the shell has already pointed at its
	// redacting logger.
	Logger *slog.Logger
}

// TraceHandler wraps a handler that is NOT built through NewServer (a plain
// net/http mux) with the same server span NewServer's listeners carry: named
// "METHOD <registered pattern>" (the mux's own pattern, never the raw path),
// with the method, the status code, the route and the listener, and no query
// string, header, body, user or org value. It also writes the same request line
// and records the same per-route request counter and duration histogram as the
// NewServer listeners (CHAOS-9106): one line per non-probe request, at Info,
// so a request that reached the server is visible there whether or not its
// span was sampled. Wrap the mux itself (or the outermost handler that passes
// the same *http.Request on), so the pattern the mux records on the request is
// the one read here.
func TraceHandler(next http.Handler, options TraceOptions) http.Handler {
	observer := spanObserver{listener: options.Listener, trustRemoteSampling: options.TrustRemoteSampling, probes: options.ProbePaths}
	if observer.probes == nil {
		observer.probes = ProbePaths
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	access := newAccessObserver(logger, options.Listener, options.TrustRemoteSampling)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, span := observer.start(r)
		if span == nil {
			next.ServeHTTP(w, r)
			return
		}
		recorder := &accessRecorder{ResponseWriter: w}
		traced := r.WithContext(context.WithValue(ctx, notFoundCauseKey{}, recorder))
		returned := false
		defer func() {
			pattern := registeredPattern(traced)
			observer.finish(span, boundedMethod(r.Method), pattern, recorder.status, !returned, recorder.notFoundCause.Load())
			access.observe(traced, pattern, recorder.status, time.Since(start))
		}()
		next.ServeHTTP(recorder, traced)
		returned = true
		if recorder.status == 0 {
			recorder.status = http.StatusOK
		}
	})
}

// registeredPattern is the pattern the mux matched, without its method prefix
// ("POST /x" -> "/x"); empty when no pattern matched.
func registeredPattern(r *http.Request) string {
	pattern := r.Pattern
	if _, rest, found := strings.Cut(pattern, " "); found {
		return rest
	}
	return pattern
}
