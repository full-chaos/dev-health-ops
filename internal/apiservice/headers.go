package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"

	"bufio"
	"errors"
	"net"
	"net/http"
	"strings"
)

// securityHeaders is the Python api's set, in its order
// (src/dev_health_ops/api/middleware/security_headers.py _DEFAULT_HEADERS).
var securityHeaders = [...][2]string{
	{"Strict-Transport-Security", "max-age=31536000; includeSubDomains"},
	{"X-Content-Type-Options", "nosniff"},
	{"X-Frame-Options", "DENY"},
	{"Referrer-Policy", "strict-origin-when-cross-origin"},
	{"Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"},
}

// SecurityHeaders adds each security header the response does not already
// carry, at the moment the status line is committed. Like the Python
// middleware it never overrides a value a handler set itself.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := &headerWriter{ResponseWriter: w, commit: addMissingSecurityHeaders}
		next.ServeHTTP(writer, r)
		// A handler that wrote nothing gets its implicit 200 from net/http
		// after this returns; the headers are still mutable here.
		writer.ensureCommitted()
	})
}

func addMissingSecurityHeaders(header http.Header) {
	for _, pair := range securityHeaders {
		if len(header.Values(pair[0])) == 0 {
			header.Set(pair[0], pair[1])
		}
	}
}

// headerWriter runs commit once, just before the status line is written,
// whichever of WriteHeader, Write or Flush comes first. A handler that writes
// nothing is covered by the middleware calling ensureCommitted after the
// handler returns.
type headerWriter struct {
	http.ResponseWriter
	commit    func(http.Header)
	committed bool
}

func (w *headerWriter) ensureCommitted() {
	if !w.committed {
		w.committed = true
		w.commit(w.ResponseWriter.Header())
	}
}

func (w *headerWriter) WriteHeader(status int) {
	w.ensureCommitted()
	w.ResponseWriter.WriteHeader(status)
}

func (w *headerWriter) Write(body []byte) (int, error) {
	w.ensureCommitted()
	return w.ResponseWriter.Write(body)
}

func (w *headerWriter) Flush() {
	w.ensureCommitted()
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *headerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *headerWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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

// UnhandledErrorShape gives a 500 for an unhandled error the Python api's
// shape: Starlette's ServerErrorMiddleware answers it outside every other
// middleware, so the response carries only its content headers (no
// security, CORS, correlation or impersonation header). It must be the
// outermost installed middleware except the provenance stamp, which sits
// outside it so scope rejections carry it too and which this shape keeps.
func UnhandledErrorShape(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&unhandledWriter{ResponseWriter: w}, r)
	})
}

type unhandledWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *unhandledWriter) commit() {
	if w.wrote {
		return
	}
	w.wrote = true
	header := w.Header()
	if header.Get(policy.UnhandledErrorHeader) == "" {
		return
	}
	for key := range header {
		switch key {
		case "Content-Type", "Content-Length", "Connection":
		case http.CanonicalHeaderKey(buildstamp.PlaneHeader), http.CanonicalHeaderKey(buildstamp.BuildHeader):
			// Provenance the prover reads from every response, error or not;
			// uvicorn behind the ingress carries neither, so nothing is lost.
		default:
			delete(header, key)
		}
	}
}

func (w *unhandledWriter) WriteHeader(status int) {
	w.commit()
	w.ResponseWriter.WriteHeader(status)
}

func (w *unhandledWriter) Write(body []byte) (int, error) {
	w.commit()
	return w.ResponseWriter.Write(body)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *unhandledWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
