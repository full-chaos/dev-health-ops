package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestRedirectSlashesIsStarlettes pins Starlette's Router redirect_slashes
// in both directions: an unmatched path whose trailing slash, toggled,
// matches a route (for any method) is a 307 to that absolute URL with an
// empty body; anything else stays a 404; and with the option off no
// answer matches Starlette's.
func TestRedirectSlashesIsStarlettes(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	build := func(on bool) *Server {
		server, err := NewServer(ServerOptions{
			Address: "127.0.0.1:0", RequestTimeout: time.Second, MaxBodyBytes: 1024, StrictPaths: true, RedirectSlashes: on,
			Routes: []Route{
				{Method: http.MethodGet, Pattern: "/items", Handler: ok},
				{Method: http.MethodPost, Pattern: "/folders/{$}", Handler: ok},
				{Method: http.MethodGet, Pattern: "/items/{id}", Handler: ok},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return server
	}
	on, off := build(true), build(false)
	for _, tc := range []struct {
		method, target, location string
		status                   int
	}{
		{"GET", "/items/", "http://example.com/items", 307},
		{"GET", "/items///", "http://example.com/items", 307},
		{"DELETE", "/items/", "http://example.com/items", 307},
		{"GET", "/folders", "http://example.com/folders/", 307},
		{"GET", "/items/a%20b/?x=1&y=%2F", "http://example.com/items/a%20b?x=1&y=%2F", 307},
		{"GET", "/items/caf%C3%A9/", "http://example.com/items/caf%C3%A9", 307},
		// Dot segments: Starlette matches the decoded path literally, so ".."
		// is an {id} value, never a parent step.
		{"GET", "/items/%2e%2e/", "http://example.com/items/..", 307},
		{"GET", "/items/%2E/", "http://example.com/items/.", 307},
		{"GET", "/items/../", "http://example.com/items/..", 307},
		{"GET", "/items/%2e%2e", "", 200},
		{"GET", "/items/%2e%2e/%2e%2e/", "", 404},
		{"GET", "/items/../items/", "", 404},
		// Invalid UTF-8: uvicorn's unquote replaces each maximal ill-formed
		// subpart with one U+FFFD before Starlette quotes the path back.
		{"GET", "/items/%FF/", "http://example.com/items/%EF%BF%BD", 307},
		{"GET", "/items/%E2%82/", "http://example.com/items/%EF%BF%BD", 307},
		{"GET", "/items/a%E2%82%ACb%C0%AF/", "http://example.com/items/a%E2%82%ACb%EF%BF%BD%EF%BF%BD", 307},
		{"GET", "/items", "", 200},
		{"GET", "/nothing/", "", 404},
		{"GET", "/nothing", "", 404},
		{"GET", "/", "", 404},
		{"GET", "/items/a/b/", "", 404},
	} {
		recorder := httptest.NewRecorder()
		on.Handler().ServeHTTP(recorder, httptest.NewRequest(tc.method, "http://example.com"+tc.target, nil))
		if recorder.Code != tc.status || recorder.Header().Get("Location") != tc.location {
			t.Errorf("on %s %s: %d Location=%q, want %d %q", tc.method, tc.target, recorder.Code, recorder.Header().Get("Location"), tc.status, tc.location)
		}
		if tc.status == 307 && (recorder.Body.Len() != 0 || recorder.Header().Get("Content-Length") != "0" || recorder.Header().Get("Content-Type") != "") {
			t.Errorf("on %s %s: body %q, headers %v", tc.method, tc.target, recorder.Body.String(), recorder.Header())
		}
		if tc.status == 307 {
			recorder = httptest.NewRecorder()
			off.Handler().ServeHTTP(recorder, httptest.NewRequest(tc.method, "http://example.com"+tc.target, nil))
			// Off, the answer is net/http's (a 404, or the mux's own relative
			// redirect with an HTML body), never Starlette's.
			if recorder.Code == tc.status && recorder.Header().Get("Location") == tc.location {
				t.Errorf("off %s %s: answered like Starlette (%d %q) without the option", tc.method, tc.target, recorder.Code, tc.location)
			}
		}
	}
}
