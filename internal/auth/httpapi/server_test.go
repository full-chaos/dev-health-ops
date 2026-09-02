package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func testOptions(routes ...Route) ServerOptions {
	return ServerOptions{
		Address:        "127.0.0.1:0",
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Routes:         routes,
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
}

func handlerFor(t *testing.T, options ServerOptions) http.Handler {
	t.Helper()
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server.Handler()
}

func decodeEnvelope(t *testing.T, response *httptest.ResponseRecorder) Envelope {
	t.Helper()
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var envelope Envelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("body %q is not an envelope: %v", response.Body.String(), err)
	}
	return envelope
}

func okRoute(method, pattern string) Route {
	return Route{
		Method:  method,
		Pattern: pattern,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}
}

// TestUnmountedServiceAnswersEveryPathWithTheNotFoundEnvelope is the dormancy
// contract as the deployed configuration actually runs it: no route mounted,
// so every path -- including ones a later wave will claim -- gets this
// service's own 404 envelope rather than net/http's plain-text default.
func TestUnmountedServiceAnswersEveryPathWithTheNotFoundEnvelope(t *testing.T) {
	handler := handlerFor(t, testOptions())

	for _, path := range []string{"/", "/v1/tokens", "/healthz", "/v1/tokens/nested/deep"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))

		if response.Code != http.StatusNotFound {
			t.Fatalf("POST %s = %d, want 404", path, response.Code)
		}
		envelope := decodeEnvelope(t, response)
		if envelope.Error.Code != CodeNotFound {
			t.Fatalf("POST %s code = %q, want %q", path, envelope.Error.Code, CodeNotFound)
		}
		if envelope.Error.RequestID == "" {
			t.Fatalf("POST %s carried no request id", path)
		}
	}
}

// TestMethodMismatchOnAKnownPathIs405WithAllow proves the ServeMux precedence
// this package relies on: the method pattern is a strict subset of the
// path-only pattern, which is a strict subset of "/", so a wrong method on a
// known path reaches the 405 fallback rather than falling through to the
// catch-all 404.
// TestDotSegmentsAreRedirectedNotServed pins http.ServeMux's own path
// cleaning as an ASSERTED behaviour rather than a surprise. A request carrying
// dot segments is answered with a 307 to the cleaned path and never reaches a
// handler, so traversal cannot select a route -- but the response is a
// redirect, not this package's 404 envelope, and a reader of the previous test
// would otherwise reasonably expect the latter.
func TestDotSegmentsAreRedirectedNotServed(t *testing.T) {
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/keys",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("a dot-segment path reached a route handler")
		}),
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/nested/../keys", nil))

	if response.Code != http.StatusTemporaryRedirect && response.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want a redirect to the cleaned path", response.Code)
	}
	if got := response.Header().Get("Location"); got != "/v1/keys" {
		t.Fatalf("Location = %q, want the cleaned path", got)
	}
	// The correlation header is still set: RequestID is outermost, so even a
	// response the mux produces on its own is correlatable.
	if response.Header().Get(RequestIDHeader) == "" {
		t.Error("the redirect carried no request id")
	}
}

func TestMethodMismatchOnAKnownPathIs405WithAllow(t *testing.T) {
	handler := handlerFor(t, testOptions(
		okRoute(http.MethodGet, "/v1/keys"),
		okRoute(http.MethodPost, "/v1/keys"),
	))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/keys", nil))

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /v1/keys = %d, want 405", response.Code)
	}
	if got := response.Header().Get("Allow"); got != "GET, POST" {
		t.Fatalf("Allow = %q, want %q", got, "GET, POST")
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodeMethodNotAllowed {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodeMethodNotAllowed)
	}

	// The registered method still wins over the path-only fallback.
	ok := httptest.NewRecorder()
	handler.ServeHTTP(ok, httptest.NewRequest(http.MethodGet, "/v1/keys", nil))
	if ok.Code != http.StatusNoContent {
		t.Fatalf("GET /v1/keys = %d, want 204", ok.Code)
	}
}

// TestHeadReachesAGetRoute pins net/http's own documented behaviour as an
// ASSERTED property rather than a surprise: since Go 1.22 a "GET" pattern also
// matches HEAD, so a HEAD request reaches the GET handler and does NOT fall to
// the 405 fallback. The Allow header on that fallback therefore lists the
// registered methods only, and reads as narrower than what is actually
// accepted -- which is worth knowing before someone treats Allow as the
// authoritative method set.
func TestHeadReachesAGetRoute(t *testing.T) {
	var reached bool
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/head",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached = true
			w.WriteHeader(http.StatusNoContent)
		}),
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/v1/head", nil))

	if !reached || response.Code != http.StatusNoContent {
		t.Fatalf("HEAD on a GET route = %d (handler reached: %t), want 204", response.Code, reached)
	}
}

// TestTrailingSlashIsADifferentPath: "/v1/keys" and "/v1/keys/" are distinct
// patterns to http.ServeMux, so the second falls to the catch-all 404 rather
// than reaching the route. Asserted so a later wave registering a route knows
// it must register both spellings if it wants both.
func TestTrailingSlashIsADifferentPath(t *testing.T) {
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/keys",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("a trailing-slash path reached the route handler")
		}),
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/keys/", nil))

	if response.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/keys/ = %d, want 404", response.Code)
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodeNotFound {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodeNotFound)
	}
}

func TestRequestIDIsEchoedAndBoundToTheContext(t *testing.T) {
	var seen string
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/echo",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = RequestIDFrom(r.Context())
			w.WriteHeader(http.StatusNoContent)
		}),
	}))

	request := httptest.NewRequest(http.MethodGet, "/v1/echo", nil)
	request.Header.Set(RequestIDHeader, "client-abc.123")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if got := response.Header().Get(RequestIDHeader); got != "client-abc.123" {
		t.Fatalf("echoed id = %q, want the client's", got)
	}
	if seen != "client-abc.123" {
		t.Fatalf("context id = %q, want the client's", seen)
	}
}

// TestUnsafeInboundRequestIDIsReplaced is the header-injection control. A
// correlation id is echoed into a response HEADER, so a CR or LF in it would
// let a caller inject a header. The value is REPLACED, never sanitised: a
// partially-repaired identifier correlates two different things under one
// name.
func TestUnsafeInboundRequestIDIsReplaced(t *testing.T) {
	handler := handlerFor(t, testOptions())

	for _, hostile := range []string{
		"abc\r\nX-Injected: yes",
		"abc\ndef",
		"space separated",
		"semi;colon",
		strings.Repeat("a", maxInboundRequestID+1),
		"",
	} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		// Set the header map directly: Header.Set would reject or mangle a
		// value containing CR/LF before this code ever saw it, which would
		// make the test prove net/http's guard rather than this one.
		request.Header[RequestIDHeader] = []string{hostile}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		got := response.Header().Get(RequestIDHeader)
		if got == hostile {
			t.Fatalf("hostile id %q was echoed back", hostile)
		}
		if got == "" {
			t.Fatalf("no id was assigned in place of %q", hostile)
		}
		if strings.ContainsAny(got, "\r\n") {
			t.Fatalf("assigned id %q contains a line break", got)
		}
		if response.Header().Get("X-Injected") != "" {
			t.Fatal("a header was injected through the request id")
		}
	}
}

func TestBodyOverTheDeclaredLimitIs413(t *testing.T) {
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/big",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("the handler ran for an oversized body")
		}),
	})
	options.MaxBodyBytes = 16
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/big", strings.NewReader(strings.Repeat("x", 64)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodePayloadTooLarge {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodePayloadTooLarge)
	}
}

// TestUndeclaredBodyLengthIsRejectedBeforeTheHandler closes the bypass a
// Content-Length check alone leaves: a chunked request declares no length at
// all (net/http represents that as ContentLength == -1).
//
// The assertion is that the handler is NEVER ENTERED, not merely that a
// handler which chooses to read gets an error. That distinction is the whole
// finding (codex round 1, P2, EXECUTED): wrapping the body in
// http.MaxBytesReader only bounds a handler that actually reads it, so a
// state-changing handler that acts without reading — or before reading — was
// never bounded at all. A guard whose enforcement depends on every future
// handler remembering to read its body is not a guard.
//
// The previous version of this test asserted only that a READING handler
// received a *http.MaxBytesError, and discarded the recorder without checking
// the status or whether the handler ran. It passed against the defect.
func TestUndeclaredBodyLengthIsRejectedBeforeTheHandler(t *testing.T) {
	var handlerRan bool
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/chunked",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Deliberately does NOT read the body, which is exactly the
			// handler shape the old implementation failed to bound.
			handlerRan = true
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.MaxBodyBytes = 16
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/chunked", strings.NewReader(strings.Repeat("x", 64)))
	// -1 is how net/http represents "length not declared", the state a chunked
	// request arrives in.
	request.ContentLength = -1
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if handlerRan {
		t.Error("an oversized undeclared-length body reached the route handler")
	}
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodePayloadTooLarge {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodePayloadTooLarge)
	}
}

// TestUndeclaredBodyLengthUnderTheLimitStillReachesTheHandler is the control
// for the test above. Rejecting every undeclared-length body would satisfy
// that assertion just as well and would break every legitimate chunked client
// — HTTP/2 clients routinely omit Content-Length — so this pins that a
// conforming request still arrives, intact and fully readable.
func TestUndeclaredBodyLengthUnderTheLimitStillReachesTheHandler(t *testing.T) {
	const payload = "under the limit"
	var seen string
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/chunked",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			seen = string(body)
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.MaxBodyBytes = 64
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/chunked", strings.NewReader(payload))
	request.ContentLength = -1
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if seen != payload {
		t.Fatalf("handler read %q, want %q", seen, payload)
	}
}

// TestBodyBoundIsExactAtTheLimit pins the off-by-one on BOTH sides, for both
// the declared and the undeclared path.
//
// Nothing else in this suite exercises a body of exactly `limit` bytes: the
// other cases are 64-against-16 (clearly over) and 15-against-64 (clearly
// under), so `len(buffered) > limit` could become `>=` -- rejecting every
// request that exactly fills the budget -- and every other test would still
// pass. Codex round 2 and the peer delta review both came back CLEAN without
// covering this boundary either, which is precisely why it is worth a test
// rather than an argument: three passes agreeing tells you nothing about a
// case none of them ran.
//
// The limit is the largest ACCEPTABLE size, so `limit` is in and `limit+1` is
// out. Reading `limit+1` bytes in the middleware is what makes those two
// distinguishable without reading an unbounded body.
func TestBodyBoundIsExactAtTheLimit(t *testing.T) {
	const limit = 32

	cases := []struct {
		name          string
		size          int
		declareLength bool
		wantStatus    int
		wantHandler   bool
	}{
		{name: "declared, exactly at the limit", size: limit, declareLength: true, wantStatus: http.StatusNoContent, wantHandler: true},
		{name: "declared, one byte over", size: limit + 1, declareLength: true, wantStatus: http.StatusRequestEntityTooLarge},
		{name: "undeclared, exactly at the limit", size: limit, wantStatus: http.StatusNoContent, wantHandler: true},
		{name: "undeclared, one byte over", size: limit + 1, wantStatus: http.StatusRequestEntityTooLarge},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var handlerRan bool
			var read int
			options := testOptions(Route{
				Method:  http.MethodPost,
				Pattern: "/v1/boundary",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handlerRan = true
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Errorf("read body: %v", err)
					}
					read = len(body)
					w.WriteHeader(http.StatusNoContent)
				}),
			})
			options.MaxBodyBytes = limit
			handler := handlerFor(t, options)

			payload := strings.Repeat("x", testCase.size)
			request := httptest.NewRequest(http.MethodPost, "/v1/boundary", strings.NewReader(payload))
			if !testCase.declareLength {
				request.ContentLength = -1
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, testCase.wantStatus)
			}
			if handlerRan != testCase.wantHandler {
				t.Fatalf("handler ran = %t, want %t", handlerRan, testCase.wantHandler)
			}
			// An accepted body must arrive WHOLE. A fix that let the request
			// through but truncated it at the bound would satisfy the status
			// assertion above and silently corrupt every request that exactly
			// fills the budget.
			if testCase.wantHandler && read != testCase.size {
				t.Fatalf("handler read %d bytes, want the whole %d-byte body", read, testCase.size)
			}
		})
	}
}

// countingReader is an endless source that records how much was taken from
// it. It exists so a test can prove the middleware STOPPED reading, which a
// finite fixture cannot: a fixture that runs out looks identical to a bound
// that worked.
type countingReader struct{ read int }

func (c *countingReader) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 'x'
	}
	c.read += len(p)
	return len(p), nil
}

// TestUndeclaredBodyReadsAtMostOneByteBeyondTheLimit proves the memory claim
// in MaxBody's doc comment against an unbounded source.
//
// Every other body test uses a fixture a few dozen bytes long, so all of them
// would pass even if the middleware drained the entire stream — the fixture
// simply ends first. This is the case that distinguishes "bounded" from
// "happened not to be very long".
//
// Construction credited to lane-auth-cp, who built it as a throwaway during
// the executed delta review of c82519bd0..5aaa29e1d; promoted here so it keeps
// holding after the review that found it is over.
func TestUndeclaredBodyReadsAtMostOneByteBeyondTheLimit(t *testing.T) {
	const limit = 16
	source := &countingReader{}

	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/endless",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("an endless oversized body reached the route handler")
		}),
	})
	options.MaxBodyBytes = limit
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/endless", source)
	request.ContentLength = -1
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
	// limit+1 is the exact budget: one byte past the limit is what makes "at
	// the limit" and "over it" distinguishable. Reading more would mean the
	// bound is not what the doc comment claims.
	if source.read > limit+1 {
		t.Fatalf("middleware read %d bytes from an endless source, want at most %d", source.read, limit+1)
	}
	if source.read == 0 {
		t.Fatal("middleware read nothing, so this test cannot distinguish a bound from a no-op")
	}
}

// failingReader yields some bytes and then fails, standing in for a client
// that disappears part-way through a body.
type failingReader struct {
	remaining int
	err       error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, f.err
	}
	take := min(len(p), f.remaining)
	for index := range take {
		p[index] = 'x'
	}
	f.remaining -= take
	return take, nil
}

// TestUndeclaredBodyReadFailureIsNotReportedAsTooLarge separates the two ways
// reading a body can end badly.
//
// A transport failure eight bytes into a sixty-four-byte budget is nowhere
// near the limit, so answering 413 would tell the caller it sent too much when
// it actually sent too little — a misdiagnosis that would send someone
// shrinking a payload that was never the problem. Buffering made these two
// outcomes share a code path, so the distinction needs a test rather than a
// reading of the code.
//
// Construction credited to lane-auth-cp (executed delta review, c82519bd0..5aaa29e1d).
func TestUndeclaredBodyReadFailureIsNotReportedAsTooLarge(t *testing.T) {
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/truncated",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("a request whose body failed mid-read reached the route handler")
		}),
	})
	options.MaxBodyBytes = 64
	handler := handlerFor(t, options)

	request := httptest.NewRequest(
		http.MethodPost, "/v1/truncated",
		&failingReader{remaining: 8, err: errors.New("connection reset")},
	)
	request.ContentLength = -1
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code == http.StatusRequestEntityTooLarge {
		t.Fatal("a short body that failed mid-read was reported as too large")
	}
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodeInvalidRequest {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodeInvalidRequest)
	}
}

// TestDeclaredBodyLengthIsStillBoundedForAReadingHandler keeps the declared
// path covered too: a body whose declared length is within the limit but which
// lies about its size must still fail the read rather than stream unbounded.
func TestDeclaredBodyLengthIsStillBoundedForAReadingHandler(t *testing.T) {
	var readErr error
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/lying",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, readErr = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.MaxBodyBytes = 16
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/lying", strings.NewReader(strings.Repeat("x", 64)))
	// Understates the real body size, so the Content-Length gate passes and
	// only the reader bound can catch it.
	request.ContentLength = 8
	handler.ServeHTTP(httptest.NewRecorder(), request)

	var maxBytes *http.MaxBytesError
	if readErr == nil {
		t.Fatal("a body that understated its Content-Length was read in full")
	}
	if !errorAs(readErr, &maxBytes) {
		t.Fatalf("read error = %v, want *http.MaxBytesError", readErr)
	}
}

func TestPanicBecomesOneInternalEnvelope(t *testing.T) {
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/boom",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic("dsn=postgres://auth:hunter2@db.internal/devhealth")
		}),
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/boom", nil))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	envelope := decodeEnvelope(t, response)
	if envelope.Error.Code != CodeInternal {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodeInternal)
	}
	// The panic VALUE must not reach the wire. This one is shaped like a
	// credential on purpose.
	if strings.Contains(response.Body.String(), "hunter2") ||
		strings.Contains(response.Body.String(), "db.internal") {
		t.Fatalf("the panic value reached the response body: %q", response.Body.String())
	}
}

// TestPanicAfterAWriteDoesNotRewriteTheStatus keeps the recovery from turning
// a truncated 200 into a lie: once a status line is committed, net/http cannot
// change it, and a second WriteHeader would log a superfluous-call warning
// while leaving the client with the original status anyway.
func TestPanicAfterAWriteDoesNotRewriteTheStatus(t *testing.T) {
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/late",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"partial":true`))
			panic("after the write")
		}),
	}))

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/late", nil))

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want the committed 202", response.Code)
	}
	if strings.Contains(response.Body.String(), string(CodeInternal)) {
		t.Fatal("an envelope was appended to an already-committed response")
	}
}

// TestErrAbortHandlerIsNotSwallowed: net/http documents ErrAbortHandler as the
// way a handler abandons a response deliberately. Converting it into a 500
// would report a server error for an intentional abort.
func TestErrAbortHandlerIsNotSwallowed(t *testing.T) {
	handler := handlerFor(t, testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/abort",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic(http.ErrAbortHandler)
		}),
	}))

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("ErrAbortHandler was swallowed")
		}
		if err, ok := recovered.(error); !ok || !errorIs(err, http.ErrAbortHandler) {
			t.Fatalf("re-panicked with %v, want http.ErrAbortHandler", recovered)
		}
	}()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/abort", nil))
}

func TestRateLimitIsPerRoute(t *testing.T) {
	now := time.Now()
	options := testOptions(
		Route{
			Method: http.MethodGet, Pattern: "/v1/hot",
			Handler:            http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }),
			RateLimitPerSecond: 1, RateLimitBurst: 2,
		},
		okRoute(http.MethodGet, "/v1/cold"),
	)
	options.Now = func() time.Time { return now }
	handler := handlerFor(t, options)

	call := func(path string) int {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		return response.Code
	}

	if got := call("/v1/hot"); got != http.StatusNoContent {
		t.Fatalf("first call = %d, want 204", got)
	}
	if got := call("/v1/hot"); got != http.StatusNoContent {
		t.Fatalf("second call = %d, want 204 (burst is 2)", got)
	}
	if got := call("/v1/hot"); got != http.StatusTooManyRequests {
		t.Fatalf("third call = %d, want 429", got)
	}
	// The neighbouring route has its own budget: exhausting one must not
	// starve the other, which is the whole point of a per-route bucket.
	if got := call("/v1/cold"); got != http.StatusNoContent {
		t.Fatalf("neighbouring route = %d, want 204", got)
	}
	// Refill.
	now = now.Add(2 * time.Second)
	if got := call("/v1/hot"); got != http.StatusNoContent {
		t.Fatalf("after refill = %d, want 204", got)
	}
}

// TestBucketDoesNotLoseTokensWhenTheClockGoesBackwards: a wall-clock step or
// an NTP correction must not make the limiter stricter than configured for an
// unbounded period.
func TestBucketDoesNotLoseTokensWhenTheClockGoesBackwards(t *testing.T) {
	now := time.Now()
	bucket := NewBucket(1, 3, func() time.Time { return now })

	if !bucket.Allow() {
		t.Fatal("the first call was denied")
	}
	now = now.Add(-time.Hour)
	for attempt := range 2 {
		if !bucket.Allow() {
			t.Fatalf("call %d after the clock went backwards was denied", attempt+2)
		}
	}
	if bucket.Allow() {
		t.Fatal("the bucket refilled from a backwards clock")
	}
}

func TestBucketIsSafeUnderConcurrency(t *testing.T) {
	const burst = 50
	bucket := NewBucket(0.0001, burst, nil)

	var allowed int
	var mu sync.Mutex
	var group sync.WaitGroup
	for range 200 {
		group.Add(1)
		go func() {
			defer group.Done()
			if bucket.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	group.Wait()

	// The refill rate is low enough that no meaningful token can accrue
	// during the test, so the burst is the ceiling. The assertion is on the
	// bound, not on an exact count, because a slow machine could legitimately
	// accrue one token.
	if allowed > burst+1 {
		t.Fatalf("allowed %d of 200 concurrent calls, want at most %d", allowed, burst+1)
	}
	if allowed < 1 {
		t.Fatal("the bucket denied every call")
	}
}

func TestNewServerRejectsMalformedRouteSets(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ServerOptions)
	}{
		{"no address", func(o *ServerOptions) { o.Address = "" }},
		{"no request timeout", func(o *ServerOptions) { o.RequestTimeout = 0 }},
		{"no body bound", func(o *ServerOptions) { o.MaxBodyBytes = 0 }},
		{"route with no handler", func(o *ServerOptions) {
			o.Routes = []Route{{Method: http.MethodGet, Pattern: "/v1/x"}}
		}},
		{"route with no method", func(o *ServerOptions) {
			o.Routes = []Route{{Pattern: "/v1/x", Handler: http.NotFoundHandler()}}
		}},
		{"route with a lowercase method", func(o *ServerOptions) {
			o.Routes = []Route{{Method: "get", Pattern: "/v1/x", Handler: http.NotFoundHandler()}}
		}},
		{"route with an unrooted pattern", func(o *ServerOptions) {
			o.Routes = []Route{{Method: http.MethodGet, Pattern: "v1/x", Handler: http.NotFoundHandler()}}
		}},
		{"the same method and path twice", func(o *ServerOptions) {
			o.Routes = []Route{okRoute(http.MethodGet, "/v1/x"), okRoute(http.MethodGet, "/v1/x")}
		}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			options := testOptions()
			testCase.mutate(&options)
			if _, err := NewServer(options); err == nil {
				t.Fatalf("NewServer accepted %s", testCase.name)
			}
		})
	}
}

// TestWriteTimeoutExceedsTheRequestDeadline: the server's WriteTimeout is the
// hard stop behind the per-request context deadline (see Deadline's doc
// comment). If it were the shorter of the two, a well-behaved handler that
// respects its deadline would have its connection torn down before it could
// render a response.
func TestWriteTimeoutExceedsTheRequestDeadline(t *testing.T) {
	options := testOptions()
	options.RequestTimeout = 3 * time.Second
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if server.server.WriteTimeout <= options.RequestTimeout {
		t.Fatalf(
			"WriteTimeout %s does not exceed RequestTimeout %s",
			server.server.WriteTimeout, options.RequestTimeout,
		)
	}
}

// TestRequestDeadlineIsBounded proves the deadline actually reaches the
// handler's context, which is what bounds every downstream DB, provider and
// key call.
func TestRequestDeadlineIsBounded(t *testing.T) {
	var deadline time.Time
	var hasDeadline bool
	options := testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/v1/deadline",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			deadline, hasDeadline = r.Context().Deadline()
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.RequestTimeout = 250 * time.Millisecond
	handler := handlerFor(t, options)

	before := time.Now()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/deadline", nil))

	if !hasDeadline {
		t.Fatal("the handler context carried no deadline")
	}
	if slack := deadline.Sub(before); slack > time.Second {
		t.Fatalf("deadline is %s away, want ~250ms", slack)
	}
}

func TestWriteErrorFallsBackToInternalForAnUnknownCode(t *testing.T) {
	response := httptest.NewRecorder()
	WriteError(response, httptest.NewRequest(http.MethodGet, "/", nil), Code("invented"))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodeInternal {
		t.Fatalf("code = %q, want %q", envelope.Error.Code, CodeInternal)
	}
}

// TestErrorResponsesAreNotCacheable: a 429 or a 404 cached by an intermediary
// outlives the condition that produced it.
func TestErrorResponsesAreNotCacheable(t *testing.T) {
	handler := handlerFor(t, testOptions())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/nowhere", nil))

	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
}
