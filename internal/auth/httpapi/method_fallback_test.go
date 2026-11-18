package httpapi

import (
	"net/http"
	"net/http/httptest"
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
