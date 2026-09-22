package apiservice

import (
	"bufio"
	"errors"
	"net"
	"net/http"
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
