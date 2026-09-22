package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

// TestStrictPathsAnswersNotFoundWhereTheMuxWouldRedirect: with StrictPaths
// every target the mux would redirect or reject reaches the writer as
// NotFound, and a canonical path is routed as before. Without it the mux's
// own redirect stands (TestDotSegmentsAreRedirectedNotServed).
func TestStrictPathsAnswersNotFoundWhereTheMuxWouldRedirect(t *testing.T) {
	options := testOptions(okRoute(http.MethodGet, "/ok"), okRoute(http.MethodGet, "/dir/"))
	options.ErrorWriter = recordingWriter
	options.StrictPaths = true
	handler := handlerFor(t, options)
	for _, target := range []string{"/a/../ok", "//ok", "/./ok", "/ok/.", "/dir/../ok", "/%2e%2e/ok"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != 599 || response.Body.String() != "custom:"+string(CodeNotFound) {
			t.Errorf("%s: %d %q, want the writer's not-found", target, response.Code, response.Body.String())
		}
	}
	star := httptest.NewRequest(http.MethodOptions, "/", nil)
	star.RequestURI, star.URL.Path = "*", "*"
	authority := httptest.NewRequest(http.MethodConnect, "/", nil)
	authority.URL.Path = ""
	for name, request := range map[string]*http.Request{"star": star, "authority form": authority} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != 599 {
			t.Errorf("%s: %d, want the writer's not-found", name, response.Code)
		}
	}
	for _, target := range []string{"/ok", "/dir/"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusNoContent {
			t.Errorf("%s: %d, want the route", target, response.Code)
		}
	}
	server, err := NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	if !server.server.DisableGeneralOptionsHandler {
		t.Fatal(`StrictPaths must let "OPTIONS *" reach the handler`)
	}
	options.StrictPaths = false
	if server, _ := NewServer(options); server.server.DisableGeneralOptionsHandler {
		t.Fatal("the general OPTIONS handler stays on without StrictPaths")
	}
}

func TestAcceptRequestIDDecidesReuse(t *testing.T) {
	options := testOptions()
	options.AcceptRequestID = func(id string) bool { return id != "" }
	handler := handlerFor(t, options)
	for _, id := range []string{"bad/value", "x y", strings.Repeat("a", 4000)} {
		request := httptest.NewRequest(http.MethodGet, "/nope", nil)
		request.Header.Set(RequestIDHeader, id)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if got := response.Header().Get(RequestIDHeader); got != id {
			t.Errorf("id %.20q replaced with %q", id, got)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if response.Header().Get(RequestIDHeader) == "" {
		t.Fatal("a missing id must still be generated")
	}
}

func TestHeaderLimitsDefaultAndOverride(t *testing.T) {
	server, err := NewServer(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if server.server.MaxHeaderBytes != 1<<16 || server.server.MaxHeaderValueCount != 0 {
		t.Fatalf("defaults changed: %d bytes, %d values", server.server.MaxHeaderBytes, server.server.MaxHeaderValueCount)
	}
	options := testOptions()
	options.MaxHeaderBytes, options.MaxHeaderValueCount = 123877, 32768
	server, err = NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	if server.server.MaxHeaderBytes != 123877 || server.server.MaxHeaderValueCount != 32768 {
		t.Fatalf("overrides not applied: %d bytes, %d values", server.server.MaxHeaderBytes, server.server.MaxHeaderValueCount)
	}
}

// TestIdleTimeoutClosesAnIdleKeepAliveConnection drives a live listener: with
// IdleTimeout set, a kept-alive connection idle past it is closed by the
// server; with the default (60 s), the same connection still serves a second
// request after the same pause.
func TestIdleTimeoutClosesAnIdleKeepAliveConnection(t *testing.T) {
	const pause = 600 * time.Millisecond
	for _, c := range []struct {
		name       string
		idle       time.Duration
		wantSecond bool
	}{
		{"set below the pause", 200 * time.Millisecond, false},
		{"default", 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			options := testOptions()
			options.IdleTimeout = c.idle
			server, err := NewServer(options)
			if err != nil {
				t.Fatal(err)
			}
			if c.idle == 0 && server.server.IdleTimeout != 60*time.Second {
				t.Fatalf("default idle timeout %v, want 60s", server.server.IdleTimeout)
			}
			if err := server.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
			conn, err := net.Dial("tcp", server.Address())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)
			ask := func() error {
				if _, err := io.WriteString(conn, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
					return err
				}
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				response, err := http.ReadResponse(reader, nil)
				if err != nil {
					return err
				}
				_, _ = io.Copy(io.Discard, response.Body)
				return response.Body.Close()
			}
			if err := ask(); err != nil {
				t.Fatalf("first request: %v", err)
			}
			time.Sleep(pause)
			err = ask()
			if c.wantSecond && err != nil {
				t.Fatalf("second request on a live connection: %v", err)
			}
			if !c.wantSecond && err == nil {
				t.Fatal("second request served on a connection idle past IdleTimeout")
			}
		})
	}
}
