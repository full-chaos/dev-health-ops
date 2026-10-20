package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestEveryRateLimitedAnswerCarriesRetryAfter covers both 429 paths of the
// route chain: the route's token bucket and the in-flight cap. Each 429 carries
// Retry-After as whole seconds, at least 1; the bucket's value is the real wait
// until it holds a token again.
func TestEveryRateLimitedAnswerCarriesRetryAfter(t *testing.T) {
	t.Run("token bucket", func(t *testing.T) {
		now := time.Unix(1_000_000, 0)
		options := testOptions(Route{Method: http.MethodGet, Pattern: "/v1/x", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})})
		options.RateLimit, options.RateLimitBurst = 0.1, 1 // one token per 10 s
		options.Now = func() time.Time { return now }
		h := handlerFor(t, options)
		first := httptest.NewRecorder()
		h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
		if first.Code != http.StatusNoContent {
			t.Fatalf("first request = %d", first.Code)
		}
		now = now.Add(2500 * time.Millisecond)
		refused := httptest.NewRecorder()
		h.ServeHTTP(refused, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
		if refused.Code != http.StatusTooManyRequests {
			t.Fatalf("second request = %d, want 429", refused.Code)
		}
		// 0.25 tokens after 2.5 s; the next token is 7.5 s away: 8 whole seconds.
		if got := refused.Header().Get("Retry-After"); got != "8" {
			t.Fatalf("Retry-After = %q, want \"8\" (the bucket's wait rounded up)", got)
		}
	})
	t.Run("in-flight cap", func(t *testing.T) {
		h := inFlightHandler(t, 1)
		var parked atomic.Int32
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			parkedPost(h, &parked, release)
		}()
		for parked.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		refused := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/x", nil)
		request.Body = io.NopCloser(http.NoBody)
		h.ServeHTTP(refused, request)
		close(release)
		wg.Wait()
		if refused.Code != http.StatusTooManyRequests {
			t.Fatalf("request over the cap = %d, want 429", refused.Code)
		}
		if seconds, err := strconv.Atoi(refused.Header().Get("Retry-After")); err != nil || seconds < 1 {
			t.Fatalf("Retry-After = %q, want whole seconds >= 1", refused.Header().Get("Retry-After"))
		}
	})
}

func TestBucketWaitIsTheTimeToTheNextToken(t *testing.T) {
	now := time.Unix(0, 0)
	bucket := NewBucket(2, 1, func() time.Time { return now })
	if ok, wait := bucket.take(); !ok || wait != 0 {
		t.Fatalf("first take = %v, %v; want true, 0", ok, wait)
	}
	if ok, wait := bucket.take(); ok || wait != 500*time.Millisecond {
		t.Fatalf("second take = %v, %v; want false, 500ms", ok, wait)
	}
	now = now.Add(500 * time.Millisecond)
	if ok, _ := bucket.take(); !ok {
		t.Fatal("a token was due after 500ms")
	}
}
