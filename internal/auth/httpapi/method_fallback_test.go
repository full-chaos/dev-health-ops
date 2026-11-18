package httpapi

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestMethodFallbackSendsUnregisteredMethodsToTheFlaggedRoute pins Route.MethodFallback:
// on a flagged pattern a method no route registers is served by the flagged
// route's chain (outer middleware and the route's Admit gate first); an
// unflagged pattern keeps its 405 with the served methods; the flag counts on
// the first route of a pattern only.
func TestMethodFallbackSendsUnregisteredMethodsToTheFlaggedRoute(t *testing.T) {
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Dispatched", r.Method+" "+r.PathValue("a")+r.PathValue("id"))
		w.Header().Set("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	refuse := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Refuse") == "outer" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	admit := func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
		if r.Header.Get("X-Refuse") == "admit" {
			w.WriteHeader(http.StatusForbidden)
			return r, false
		}
		return r, true
	}
	server, err := NewServer(ServerOptions{
		Address: "127.0.0.1:0", RequestTimeout: time.Second, MaxBodyBytes: 1024, StrictPaths: true, ExplicitHead: true,
		Middleware: []func(http.Handler) http.Handler{refuse},
		Routes: []Route{
			{Method: http.MethodGet, Pattern: "/pair/{a}/{b}", Handler: dispatch, MethodFallback: true, Admit: admit},
			{Method: http.MethodPost, Pattern: "/pair/{a}/{b}", Handler: dispatch, MethodFallback: true, Admit: admit},
			{Method: http.MethodPost, Pattern: "/plain", Handler: ok},
			{Method: http.MethodGet, Pattern: "/plain", Handler: ok},
			{Method: http.MethodPost, Pattern: "/late/{id}", Handler: ok},
			{Method: http.MethodGet, Pattern: "/late/{id}", Handler: dispatch, MethodFallback: true},
			{Method: http.MethodGet, Pattern: "/headless/{id}", Handler: dispatch, MethodFallback: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, refuse string
		status               int
		allow, dispatched    string
	}{
		{"TRACE", "/pair/x/y", "", 405, "POST", "TRACE x"},
		{"PROPFIND", "/pair/x/y", "", 405, "POST", "PROPFIND x"},
		{"CUSTOM", "/pair/x/y", "", 405, "POST", "CUSTOM x"},
		{"CONNECT", "/pair/x/y", "", 405, "POST", "CONNECT x"},
		{"TRACE", "/pair/x/y", "outer", 401, "", ""},
		{"TRACE", "/pair/x/y", "admit", 403, "", ""},
		{"GET", "/pair/x/y", "", 405, "POST", "GET x"},
		{"TRACE", "/plain", "", 405, "GET, POST", ""},
		{"TRACE", "/late/1", "", 405, "GET, POST", ""},
		{"HEAD", "/headless/1", "", 405, "POST", "HEAD 1"},
		{"TRACE", "/nowhere", "", 404, "", ""},
	} {
		request := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.refuse != "" {
			request.Header.Set("X-Refuse", tc.refuse)
		}
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, request)
		if rec.Code != tc.status || rec.Header().Get("Allow") != tc.allow || rec.Header().Get("X-Dispatched") != tc.dispatched {
			t.Errorf("%s %s refuse=%q: %d allow=%q dispatched=%q, want %d allow=%q dispatched=%q", tc.method, tc.path, tc.refuse,
				rec.Code, rec.Header().Get("Allow"), rec.Header().Get("X-Dispatched"), tc.status, tc.allow, tc.dispatched)
		}
	}
}

type fallbackBody struct {
	io.Reader
	bytes int
}

func (b *fallbackBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.bytes += n
	return n, err
}

func (b *fallbackBody) Close() error { return nil }

// TestMethodFallbackBoundsTheBodyLazily: the fallback reads no body before
// the handler on any framing or declared length (the handler answers first,
// as the router's 405 did) and bounds the reader the handler gets; the
// route's own method keeps the eager bound (a 413 before the handler).
func TestMethodFallbackBoundsTheBodyLazily(t *testing.T) {
	var readInHandler int64
	var readErr error
	var reached bool
	var body *fallbackBody
	before := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached, before = true, body.bytes
		if r.Header.Get("X-Read") != "" {
			var n int64
			n, readErr = io.Copy(io.Discard, r.Body)
			readInHandler = n
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	server, err := NewServer(ServerOptions{
		Address: "127.0.0.1:0", RequestTimeout: time.Second, MaxBodyBytes: 4, StrictPaths: true, ExplicitHead: true,
		Routes: []Route{{Method: http.MethodPost, Pattern: "/pair/{a}", Handler: handler, MethodFallback: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, method, body, read string
		length                   int64
		transfer                 []string
		status                   int
		reached                  bool
		before                   int
		inHandler                int64
		tooLarge                 bool
	}{
		{"chunked unread", "TRACE", "{}", "", -1, []string{"chunked"}, 405, true, 0, 0, false},
		{"undeclared unread", "TRACE", "{}", "", -1, nil, 405, true, 0, 0, false},
		{"declared unread", "TRACE", "{}", "", 2, nil, 405, true, 0, 0, false},
		{"declared over the bound unread", "TRACE", "123456", "", 6, nil, 405, true, 0, 0, false},
		{"chunked over the bound read by the handler", "TRACE", "123456", "1", -1, []string{"chunked"}, 405, true, 0, 4, true},
		{"declared over the bound read by the handler", "PROPFIND", "123456", "1", 6, nil, 405, true, 0, 4, true},
		{"registered declared over the bound", "POST", "123456", "", 6, nil, http.StatusRequestEntityTooLarge, false, 0, 0, false},
		{"registered chunked over the bound", "POST", "123456", "", -1, []string{"chunked"}, http.StatusRequestEntityTooLarge, false, 5, 0, false},
	} {
		readInHandler, readErr, reached = 0, nil, false
		body = &fallbackBody{Reader: strings.NewReader(tc.body)}
		request := httptest.NewRequest(tc.method, "/pair/x", nil)
		request.Body, request.ContentLength, request.TransferEncoding = body, tc.length, tc.transfer
		if tc.read != "" {
			request.Header.Set("X-Read", tc.read)
		}
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, request)
		if !reached {
			before = body.bytes
		}
		var maxErr *http.MaxBytesError
		if rec.Code != tc.status || reached != tc.reached || before != tc.before || readInHandler != tc.inHandler || errors.As(readErr, &maxErr) != tc.tooLarge {
			t.Errorf("%s: %d reached=%v, %d bytes read before the handler, %d in it (err %v); want %d reached=%v, %d, %d, too large %v",
				tc.name, rec.Code, reached, before, readInHandler, readErr, tc.status, tc.reached, tc.before, tc.inHandler, tc.tooLarge)
		}
	}
}
