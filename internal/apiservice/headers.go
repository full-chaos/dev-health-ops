package apiservice

import (
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"

	"net/http"
	"strings"
)

// CloseHTTP10 answers an HTTP/1.0 request with "Connection: close" and so
// closes the connection after the response, as the Python api's server does
// (h11 never keeps an HTTP/1.0 connection alive, whatever the request asks).
// net/http would otherwise honour an HTTP/1.0 keep-alive request.
func CloseHTTP10(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.ProtoAtLeast(1, 1) {
			w.Header().Set("Connection", "close")
		}
		next.ServeHTTP(w, r)
	})
}

// DecodedPathRouting routes on the percent-decoded path, as Starlette does:
// an encoded slash ("%2F") in a request target is a path separator for
// route matching, so /x/a%2Fb matches no one-segment {param} route and gets
// the 404, before any authentication. net/http's mux would otherwise keep
// "a%2Fb" as one segment.
func DecodedPathRouting(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawPath != "" && strings.Contains(strings.ToLower(r.URL.RawPath), "%2f") {
			clone := *r.URL
			clone.RawPath = ""
			r = r.Clone(r.Context())
			r.URL = &clone
		}
		next.ServeHTTP(w, r)
	})
}

// EdgeUnhandledErrorShape gives the billing edge's 500 for an unhandled
// error the shape of a bare Starlette app: ServerErrorMiddleware answers with
// the plain text "Internal Server Error" (the main app's JSON body comes
// from an exception handler the edge app does not have), carrying only its
// content headers.
func EdgeUnhandledErrorShape(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&edgeUnhandledWriter{ResponseWriter: w}, r)
	})
}

type edgeUnhandledWriter struct {
	http.ResponseWriter
	decided, plain, written bool
}

const edgeUnhandledBody = "Internal Server Error"

func (w *edgeUnhandledWriter) decide() {
	if w.decided {
		return
	}
	w.decided = true
	header := w.Header()
	if header.Get(policy.UnhandledErrorHeader) == "" {
		return
	}
	w.plain = true
	for key := range header {
		delete(header, key)
	}
	header.Set("Content-Type", "text/plain; charset=utf-8")
	header.Set("Content-Length", fmt.Sprint(len(edgeUnhandledBody)))
}

func (w *edgeUnhandledWriter) WriteHeader(status int) {
	w.decide()
	w.ResponseWriter.WriteHeader(status)
}

func (w *edgeUnhandledWriter) Write(body []byte) (int, error) {
	w.decide()
	if !w.plain {
		return w.ResponseWriter.Write(body)
	}
	if !w.written {
		w.written = true
		if _, err := w.ResponseWriter.Write([]byte(edgeUnhandledBody)); err != nil {
			return 0, err
		}
	}
	return len(body), nil
}
