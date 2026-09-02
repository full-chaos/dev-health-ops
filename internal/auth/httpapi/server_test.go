package httpapi

import (
	"encoding/json"
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

// TestUndeclaredBodyLengthIsStillBounded closes the bypass a Content-Length
// check alone leaves: a chunked request declares no length at all, so the body
// itself has to be bounded too.
func TestUndeclaredBodyLengthIsStillBounded(t *testing.T) {
	var readErr error
	options := testOptions(Route{
		Method:  http.MethodPost,
		Pattern: "/v1/chunked",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, readErr = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	options.MaxBodyBytes = 16
	handler := handlerFor(t, options)

	request := httptest.NewRequest(http.MethodPost, "/v1/chunked", strings.NewReader(strings.Repeat("x", 64)))
	// -1 is how net/http represents "length not declared", which is the state
	// a chunked request arrives in.
	request.ContentLength = -1
	handler.ServeHTTP(httptest.NewRecorder(), request)

	var maxBytes *http.MaxBytesError
	if readErr == nil {
		t.Fatal("an undeclared oversized body was read in full")
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
