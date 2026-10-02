package httpapi

import (
	"net/http"
	"strings"
)

// TraceOptions configures TraceHandler.
type TraceOptions struct {
	// Listener is the dev_health.listener attribute ("public", "internal", ...).
	Listener string
	// TrustRemoteSampling extracts and honours an incoming traceparent (a
	// listener whose callers are in-cluster). Off, the incoming traceparent is
	// ignored and every request starts a new root span.
	TrustRemoteSampling bool
	// ProbePaths are the exact paths that get no span. Nil means ProbePaths.
	ProbePaths []string
}

// TraceHandler wraps a handler that is NOT built through NewServer (a plain
// net/http mux) with the same server span NewServer's listeners carry: named
// "METHOD <registered pattern>" (the mux's own pattern, never the raw path),
// with the method, the status code, the route and the listener, and no query
// string, header, body, client address, user or org value. Wrap the mux itself
// (or the outermost handler that passes the same *http.Request on), so the
// pattern the mux records on the request is the one read here.
func TraceHandler(next http.Handler, options TraceOptions) http.Handler {
	observer := spanObserver{listener: options.Listener, trustRemoteSampling: options.TrustRemoteSampling, probes: options.ProbePaths}
	if observer.probes == nil {
		observer.probes = ProbePaths
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := observer.start(r)
		if span == nil {
			next.ServeHTTP(w, r)
			return
		}
		recorder := &accessRecorder{ResponseWriter: w}
		traced := r.WithContext(ctx)
		returned := false
		defer func() {
			observer.finish(span, boundedMethod(r.Method), registeredPattern(traced), recorder.status, !returned)
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
