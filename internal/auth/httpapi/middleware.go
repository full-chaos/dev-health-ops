package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// RequestIDHeader is the inbound and outbound correlation header.
const RequestIDHeader = "X-Request-Id"

// maxInboundRequestID bounds an accepted client-supplied correlation id. A
// correlation id is echoed into a response HEADER, so it is untrusted input on
// a header-injection path: anything outside the accepted charset (which
// excludes CR and LF by construction) or over this length is replaced by a
// freshly generated id rather than sanitised, because a partially-repaired
// identifier correlates two different things under one name.
const maxInboundRequestID = 128

type requestIDKey struct{}

// RequestIDFrom returns the correlation id bound to ctx, or "" when the
// request did not pass through RequestID.
func RequestIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

// acceptableRequestID reports whether a client-supplied id may be reused.
//
// The charset is deliberately narrow -- unreserved URL characters -- so the
// value is safe in a header, in a log line and in a JSON body without any call
// site having to escape it. CR and LF are outside it, which is what closes the
// header-injection path.
func acceptableRequestID(value string) bool {
	if value == "" || len(value) > maxInboundRequestID {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.', r == ':':
		default:
			return false
		}
	}
	return true
}

// RequestID binds a correlation id to the request context and echoes it on the
// response. A usable inbound id is preserved so a caller can correlate across
// services; anything else is replaced.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !acceptableRequestID(id) {
			id = uuid.NewString()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

// Recover turns a panicking handler into one internal-error envelope and one
// log line, and never lets the panic reach net/http's own recovery (which
// closes the connection without a response).
//
// http.ErrAbortHandler is re-panicked rather than swallowed: it is net/http's
// documented way for a handler to abandon a response deliberately, and
// converting it into a 500 would turn an intentional abort into a reported
// server error.
func Recover(logger *slog.Logger, pattern string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := &statusRecorder{ResponseWriter: w}
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if err, ok := recovered.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(recovered)
				}
				// The panic VALUE is never logged: it is arbitrary content
				// from whatever panicked and has no bounded shape. The route
				// pattern and the request id are what an operator needs to
				// find the stack in the runtime's own crash output.
				logger.LogAttrs(
					r.Context(), slog.LevelError, "handler panicked",
					append(logAttrs(r, pattern), slog.String("error_category", "handler_panic"))...,
				)
				if recorder.wrote {
					// The handler already committed a status; the response is
					// truncated but its status line is not ours to change.
					return
				}
				WriteError(recorder, r, CodeInternal)
			}()
			next.ServeHTTP(recorder, r)
		})
	}
}

// MaxBody bounds the request body BEFORE the handler runs, in both of the two
// shapes a request can arrive in.
//
// A declared Content-Length over the limit is rejected outright, so an
// oversized upload costs nothing.
//
// A body with NO declared length (chunked transfer encoding, which net/http
// represents as ContentLength == -1, and which HTTP/2 clients routinely send)
// cannot be judged without reading, so this reads up to limit+1 bytes and
// decides then. Over the limit is a 413 and the handler never runs; within it,
// the buffered bytes are handed to the handler as an ordinary body, so a
// conforming chunked client is unaffected.
//
// The earlier implementation wrapped an undeclared-length body in
// http.MaxBytesReader and called the handler immediately. That bounds only a
// handler that actually READS the body: a handler acting before or without
// reading was never bounded at all, and a 64-byte chunked body against a
// 16-byte limit reached a route handler which returned 204 (codex round 1, P2,
// reproduced independently by this lane; the previous test passed against the
// defect because it asserted only that a READING handler received an error).
// A guard whose enforcement depends on every future handler remembering to
// read its body is not a guard — which matters more here than in most places,
// because no route is mounted yet, so every handler this bound will ever
// protect is still unwritten.
//
// The cost is explicit: an undeclared-length request is buffered in memory up
// to the limit. That is bounded per request by the limit itself, and in
// aggregate by the route's rate limiter and the server's connection limits;
// it is the price of a guarantee that does not depend on handler discipline.
// A future route needing true streaming ingest wants its own middleware, not a
// weakening of this one.
func MaxBody(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > limit {
				WriteError(w, r, CodePayloadTooLarge)
				return
			}
			if r.Body == nil {
				next.ServeHTTP(w, r)
				return
			}
			if r.ContentLength >= 0 {
				// The declared length is within the limit, but a body may
				// still understate it, so the reader stays bounded too.
				r.Body = http.MaxBytesReader(w, r.Body, limit)
				next.ServeHTTP(w, r)
				return
			}

			// Undeclared length: read one byte past the limit so "exactly at
			// the limit" and "over it" are distinguishable.
			buffered, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
			if err != nil {
				// A transport failure reading the body, not a size violation.
				// The client is usually already gone; answer honestly rather
				// than reporting a size problem that did not occur.
				WriteError(w, r, CodeInvalidRequest)
				return
			}
			if int64(len(buffered)) > limit {
				WriteError(w, r, CodePayloadTooLarge)
				return
			}
			// Hand the handler an ordinary, fully-readable body. net/http owns
			// draining and closing the original; replacing the reader here does
			// not change that.
			r.Body = io.NopCloser(bytes.NewReader(buffered))
			r.ContentLength = int64(len(buffered))
			next.ServeHTTP(w, r)
		})
	}
}

// Deadline bounds the request context.
//
// This is the mechanism behind CHAOS-4881's "bounded context.Context deadlines
// through every DB/provider/key call": every downstream call in a handler
// derives from this context, so none of them can outlive the request that
// caused them.
//
// It deliberately does NOT substitute a timeout response the way
// http.TimeoutHandler does. That handler buffers the inner response and swaps
// in its own on expiry, which cannot cancel the handler goroutine either, and
// buys a double-write hazard plus a body this package could not render as an
// envelope. The hard stop for a handler that ignores its context is the
// server's WriteTimeout, set in NewServer, which ends the connection; the
// client is never left hanging.
func Deadline(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimit rejects a request when the route's bucket is empty.
//
// The bucket is PER ROUTE, not per client: this is an availability control
// protecting the service's own dependencies, so each registered route gets its
// own budget and one hot route cannot exhaust another's. Per-caller quota is a
// policy concern for a later wave and needs an authenticated principal, which
// this dormant wave does not have.
func RateLimit(limiter *Bucket) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter != nil && !limiter.Allow() {
				WriteError(w, r, CodeRateLimited)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Bucket is a token bucket.
//
// Hand-written rather than golang.org/x/time/rate on purpose: ACP-ADR-01's
// standing rule is standard library first with explicit review and pinning of
// every added dependency, and this is forty lines with a single lock. It is
// not a hand-rolled cryptographic primitive -- the thing that rule actually
// forbids.
type Bucket struct {
	perSecond float64
	burst     float64
	now       func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewBucket returns a bucket that starts full. A non-positive rate or burst
// yields a nil bucket, which RateLimit treats as "no limit" -- the caller
// validates its configuration (authconfig bounds both), so this is the
// degenerate case, not a supported way to disable the limit silently.
func NewBucket(perSecond float64, burst int, now func() time.Time) *Bucket {
	if perSecond <= 0 || burst <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &Bucket{
		perSecond: perSecond,
		burst:     float64(burst),
		now:       now,
		tokens:    float64(burst),
		last:      now(),
	}
}

// Allow consumes one token, refilling first for the time elapsed since the
// last call.
//
// Elapsed time is clamped at zero: a clock that moves backwards (a manual
// step, an NTP correction, or a test injecting a fixed instant) must not
// SUBTRACT tokens, which would make the limiter stricter than configured for
// an unbounded period. time.Time.Sub uses the monotonic reading when both
// values carry one, so this is a guard against the cases where it cannot,
// not a claim that wall-clock jumps are the normal path.
func (b *Bucket) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += elapsed.Seconds() * b.perSecond
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// statusRecorder tracks whether a status line has been committed, so Recover
// can tell "the handler panicked before writing anything" (an envelope is
// still possible) from "the handler panicked mid-response" (it is not).
//
// Unwrap lets http.ResponseController reach the underlying writer, so a
// handler needing Flush, Hijack or a per-request deadline is not blocked by
// this wrapper -- the modern replacement for re-implementing every optional
// interface here and silently dropping the ones that were forgotten.
type statusRecorder struct {
	http.ResponseWriter
	wrote bool
}

func (s *statusRecorder) WriteHeader(status int) {
	if s.wrote {
		return
	}
	s.wrote = true
	s.ResponseWriter.WriteHeader(status)
}

func (s *statusRecorder) Write(data []byte) (int, error) {
	// An implicit 200 counts as committed: net/http writes the status line on
	// the first Write, so anything after it cannot change the status either.
	s.wrote = true
	return s.ResponseWriter.Write(data)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
