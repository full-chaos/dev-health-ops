package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestCatchAllMethodsFollowStarlettesPartialMatch pins the routing of an app
// with a catch-all route after its own routes (the Python billing edge): a
// method the catch-all takes is its 404 on any path a route does not serve,
// a method outside the list is the first route path's 405 with that route's
// Allow, and on a path no route serves the catch-all's 405 with its methods
// (sorted) as Allow.
func TestCatchAllMethodsFollowStarlettesPartialMatch(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server, err := NewServer(ServerOptions{
		Address: "127.0.0.1:0", RequestTimeout: time.Second, MaxBodyBytes: 1024, StrictPaths: true, ExplicitHead: true,
		CatchAllMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"},
		Routes: []Route{
			{Method: http.MethodPost, Pattern: "/hook", Handler: ok},
			{Method: http.MethodGet, Pattern: "/health", Handler: ok},
			{Method: http.MethodHead, Pattern: "/health", Handler: ok},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	const catchAllAllow = "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"
	for _, tc := range []struct {
		method, path string
		status       int
		allow        string
	}{
		{"POST", "/hook", 200, ""},
		{"GET", "/health", 200, ""},
		{"HEAD", "/health", 200, ""},
		{"GET", "/hook", 404, ""},
		{"PUT", "/health", 404, ""},
		{"OPTIONS", "/health", 404, ""},
		{"GET", "/anything", 404, ""},
		{"DELETE", "/", 404, ""},
		{"TRACE", "/hook", 405, "POST"},
		{"PROPFIND", "/health", 405, "GET, HEAD"},
		{"TRACE", "/anything", 405, catchAllAllow},
		{"PURGE", "/", 405, catchAllAllow},
	} {
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != tc.status || rec.Header().Get("Allow") != tc.allow {
			t.Errorf("%s %s: %d allow=%q, want %d allow=%q", tc.method, tc.path, rec.Code, rec.Header().Get("Allow"), tc.status, tc.allow)
		}
	}
}
