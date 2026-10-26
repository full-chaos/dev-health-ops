package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func unthrottledServer(t *testing.T, release chan struct{}, entered chan struct{}) http.Handler {
	t.Helper()
	now := time.Now()
	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	hold := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	options := testOptions(
		Route{Method: http.MethodGet, Pattern: "/stub/a", Handler: stub, Unthrottled: true},
		Route{Method: http.MethodPost, Pattern: "/stub/b", Handler: stub, Unthrottled: true},
		okRoute(http.MethodGet, "/v1/plain"),
		Route{Method: http.MethodGet, Pattern: "/v1/hold", Handler: hold},
	)
	options.RateLimit, options.RateLimitBurst, options.MaxInFlight = 1, 1, 1
	options.Now = func() time.Time { return now }
	return handlerFor(t, options)
}

func TestUnthrottledRoutesAnswerWithAnEmptyBucketAndAFullInFlightPool(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	h := unthrottledServer(t, release, entered)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/hold", nil))
		close(done)
	}()
	<-entered
	defer func() { close(release); <-done }()
	for i := 0; i < 5; i++ {
		for _, rt := range []struct{ method, path string }{{http.MethodGet, "/stub/a"}, {http.MethodPost, "/stub/b"}} {
			req := httptest.NewRequest(rt.method, rt.path, strings.NewReader("x"))
			req.ContentLength = 1 << 30
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Fatalf("%s %s: %d Retry-After %q; want 503 and 1", rt.method, rt.path, rec.Code, rec.Header().Get("Retry-After"))
			}
		}
	}
}

func TestOtherRoutesStillAreRateLimitedBesideUnthrottledOnes(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	close(release)
	h := unthrottledServer(t, release, entered)
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/stub/a", nil))
	}
	if got := call(h, http.MethodGet, "/v1/plain").Code; got != http.StatusNoContent {
		t.Fatalf("first plain call after stub calls = %d, want 204", got)
	}
	if got := call(h, http.MethodGet, "/v1/plain").Code; got != http.StatusTooManyRequests {
		t.Fatalf("second plain call = %d, want 429", got)
	}
	if got := call(h, http.MethodPost, "/stub/b").Code; got != http.StatusServiceUnavailable {
		t.Fatalf("stub after plain 429 = %d", got)
	}
}

func call(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader("")))
	return rec
}
