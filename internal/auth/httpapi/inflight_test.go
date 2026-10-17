package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type parkedBody struct {
	release chan struct{}
	parked  *atomic.Int32
}

func (b parkedBody) Read([]byte) (int, error) {
	b.parked.Add(1)
	<-b.release
	return 0, io.EOF
}

func parkedPost(h http.Handler, parked *atomic.Int32, release chan struct{}) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/x", nil)
	req.Body = io.NopCloser(parkedBody{release, parked})
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func inFlightHandler(t *testing.T, limit int) http.Handler {
	options := testOptions(Route{Method: http.MethodPost, Pattern: "/v1/x", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})})
	options.MaxInFlight = limit
	return handlerFor(t, options)
}

// TestMaxInFlightCapsConcurrentBodyBuffering is the CHAOS-4893 construction:
// 64 requests whose bodies block, through the real RateLimit + MaxBody chain
// with the rate limit generous enough to admit all of them.
func TestMaxInFlightCapsConcurrentBodyBuffering(t *testing.T) {
	const total, limit = 64, 8
	h := inFlightHandler(t, limit)
	var parked atomic.Int32
	release := make(chan struct{})
	codes := make(chan int, total)
	var retryAfter atomic.Value
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := parkedPost(h, &parked, release)
			if rec.Code == http.StatusTooManyRequests {
				retryAfter.Store(rec.Header().Get("Retry-After"))
			}
			codes <- rec.Code
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(codes) < total-limit && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if got := parked.Load(); got != limit {
		t.Errorf("parked in body read = %d, want exactly the cap %d", got, limit)
	}
	if got := len(codes); got != total-limit {
		t.Errorf("refused so far = %d, want %d", got, total-limit)
	}
	t.Logf("cap %d of %d: %d parked in body read, %d refused with 429", limit, total, parked.Load(), len(codes))
	close(release)
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[http.StatusNoContent] != limit || counts[http.StatusTooManyRequests] != total-limit {
		t.Errorf("final codes = %v, want %d x 204 and %d x 429", counts, limit, total-limit)
	}
	if v, _ := retryAfter.Load().(string); v != "1" {
		t.Errorf("Retry-After = %q, want 1", v)
	}

	// Slots are released: a later request is admitted.
	again := make(chan struct{})
	close(again)
	if rec := parkedPost(h, &parked, again); rec.Code != http.StatusNoContent {
		t.Errorf("after release: %d, want 204", rec.Code)
	}
}

func TestZeroMaxInFlightIsUncapped(t *testing.T) {
	h := inFlightHandler(t, 0)
	var parked atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); parkedPost(h, &parked, release) }()
	}
	time.Sleep(300 * time.Millisecond)
	if got := parked.Load(); got != 64 {
		t.Errorf("parked = %d, want 64 (no cap)", got)
	}
	close(release)
	wg.Wait()
}
