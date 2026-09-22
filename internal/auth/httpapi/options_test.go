package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// recordingWriter is an ErrorWriter that renders a marker body, so a test can
// tell which writer answered.
func recordingWriter(w http.ResponseWriter, _ *http.Request, code Code) {
	w.WriteHeader(599)
	_, _ = w.Write([]byte("custom:" + string(code)))
}

// TestErrorWriterAnswersEveryErrorTheServerEmits drives each error path of a
// server built with ErrorWriter and requires the custom writer, never the
// envelope, to answer it.
func TestErrorWriterAnswersEveryErrorTheServerEmits(t *testing.T) {
	panicking := Route{Method: http.MethodGet, Pattern: "/panic", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})}
	limited := okRoute(http.MethodPost, "/limited")
	limited.RateLimitPerSecond, limited.RateLimitBurst = 0.001, 1
	options := testOptions(okRoute(http.MethodPost, "/body"), panicking, limited)
	options.ErrorWriter = recordingWriter
	handler := handlerFor(t, options)

	cases := []struct {
		name    string
		request func() *http.Request
		code    Code
	}{
		{"not found", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/nope", nil) }, CodeNotFound},
		{"method not allowed", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/body", nil) }, CodeMethodNotAllowed},
		{"declared body too large", func() *http.Request {
			return httptest.NewRequest(http.MethodPost, "/body", strings.NewReader(strings.Repeat("x", 2048)))
		}, CodePayloadTooLarge},
		{"undeclared body too large", func() *http.Request {
			request := httptest.NewRequest(http.MethodPost, "/body", strings.NewReader(strings.Repeat("x", 2048)))
			request.ContentLength = -1
			return request
		}, CodePayloadTooLarge},
		{"undeclared body read failure", func() *http.Request {
			request := httptest.NewRequest(http.MethodPost, "/body", brokenBody{})
			request.ContentLength = -1
			return request
		}, CodeInvalidRequest},
		{"panic", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/panic", nil) }, CodeInternal},
	}
	for _, test := range cases {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, test.request())
		if response.Code != 599 || response.Body.String() != "custom:"+string(test.code) {
			t.Errorf("%s: %d %q, want the custom writer for %s", test.name, response.Code, response.Body.String(), test.code)
		}
	}
	// Rate limit: the first request takes the only token, the second is limited.
	for index, want := range []int{http.StatusNoContent, 599} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/limited", nil))
		if response.Code != want {
			t.Fatalf("rate-limited request %d: %d, want %d", index, response.Code, want)
		}
		if want == 599 && response.Body.String() != "custom:"+string(CodeRateLimited) {
			t.Fatalf("rate limit rendered %q", response.Body.String())
		}
	}
}

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, context.Canceled }

func TestDefaultErrorWriterIsTheEnvelope(t *testing.T) {
	response := httptest.NewRecorder()
	handlerFor(t, testOptions()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if envelope := decodeEnvelope(t, response); envelope.Error.Code != CodeNotFound {
		t.Fatalf("default writer rendered %+v", envelope)
	}
}

// TestMiddlewareWrapsTheMuxInOrder: Middleware[0] sees the request first, all
// of it runs inside RequestID (the id is already bound) and for unrouted
// paths too.
func TestMiddlewareWrapsTheMuxInOrder(t *testing.T) {
	var order bytes.Buffer
	mark := func(label string) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if RequestIDFrom(r.Context()) == "" {
					t.Errorf("%s ran before the request id was bound", label)
				}
				order.WriteString(label)
				next.ServeHTTP(w, r)
			})
		}
	}
	options := testOptions(okRoute(http.MethodGet, "/ok"))
	options.Middleware = []func(http.Handler) http.Handler{mark("a"), mark("b")}
	handler := handlerFor(t, options)
	for _, path := range []string{"/ok", "/unrouted"} {
		order.Reset()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		if order.String() != "ab" {
			t.Fatalf("%s: middleware order %q, want ab", path, order.String())
		}
	}
}

func TestNilMiddlewareIsRefused(t *testing.T) {
	options := testOptions()
	options.Middleware = []func(http.Handler) http.Handler{nil}
	if _, err := NewServer(options); err == nil {
		t.Fatal("a nil middleware must be refused at construction")
	}
}

func TestServerNameDefaultsAndOverrides(t *testing.T) {
	for name, want := range map[string]string{"": "auth-api-http", "api-http": "api-http"} {
		options := testOptions()
		options.Name = name
		server, err := NewServer(options)
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		if server.Name() != want {
			t.Fatalf("name %q, want %q", server.Name(), want)
		}
	}
}
