package authapi

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/token"
)

// jwksMaxStaleWant is the ticket's bound: last-good is served for at most 30 s
// after the last successful read began.
const jwksMaxStaleWant = 30 * time.Second

// scriptedKeys is a key store a test drives: a set, a failure, or a hang that
// lasts until release or the read's context ends. Compromised is its latch.
type scriptedKeys struct {
	mu      sync.Mutex
	set     token.KeySet
	err     error
	hang    chan struct{}
	denied  map[string]bool
	calls   atomic.Int64
	pending atomic.Int64
}

func (s *scriptedKeys) KeySet(ctx context.Context) (token.KeySet, error) {
	s.calls.Add(1)
	s.mu.Lock()
	hang := s.hang
	s.mu.Unlock()
	if hang != nil {
		s.pending.Add(1)
		select {
		case <-hang:
		case <-ctx.Done():
			s.pending.Add(-1)
			return token.KeySet{}, ctx.Err()
		}
		s.pending.Add(-1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set, s.err
}

func (s *scriptedKeys) Compromised(kid string, _ ed25519.PublicKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.denied[kid]
}

func (s *scriptedKeys) serve(set token.KeySet, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set, s.err = set, err
}

func (s *scriptedKeys) hangReads() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hang = make(chan struct{})
}

func (s *scriptedKeys) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hang != nil {
		close(s.hang)
		s.hang = nil
	}
}

func (s *scriptedKeys) latch(kid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denied == nil {
		s.denied = map[string]bool{}
	}
	s.denied[kid] = true
}

// get is one JWKS GET whose request context ends after timeout.
func (h *jwksHarness) get(timeout time.Duration) *httptest.ResponseRecorder {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, PatternJWKS, nil).WithContext(ctx))
	return recorder
}

// eventually polls cond until it holds or wait passes.
func eventually(wait time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func startedHarness(t *testing.T, keys KeySetSource, refreshEvery time.Duration) *jwksHarness {
	t.Helper()
	h := newJWKSHarness(t, keys, refreshEvery)
	h.start(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.stop(ctx)
	})
	return h
}

// A JWKS request reads the document in memory only: no request reaches the
// key store.
func TestJWKSRequestsNeverReadTheKeyStore(t *testing.T) {
	set, _ := twoKeySet(t)
	keys := &scriptedKeys{set: set}
	h := newJWKSHarness(t, keys, time.Hour)
	before := keys.calls.Load()
	for range 100 {
		if rec := h.get(time.Second); rec.Code != http.StatusOK {
			t.Fatalf("status %d, want 200", rec.Code)
		}
	}
	if reads := keys.calls.Load() - before; reads != 0 {
		t.Fatalf("100 JWKS requests read the key store %d times, want 0", reads)
	}
}

// A hung key store holds no JWKS request: every request answers the last-known
// document, then 503 with Retry-After once it is older than the bound, and 200
// again after the store answers.
func TestJWKSHungKeyStoreServesLastKnownThen503AndRecovers(t *testing.T) {
	set, _ := twoKeySet(t)
	keys := &scriptedKeys{set: set}
	h := startedHarness(t, keys, 10*time.Millisecond)
	keys.hangReads()
	if !eventually(5*time.Second, func() bool { return keys.pending.Load() > 0 }) {
		t.Log("no refresh entered the hung read")
	}
	codes := make([]int, 500)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = h.get(time.Second).Code
		}()
	}
	wg.Wait()
	refused := 0
	for _, code := range codes {
		if code != http.StatusOK {
			refused++
		}
	}
	if refused != 0 {
		t.Fatalf("%d of 500 JWKS requests did not answer 200 while the key store hung", refused)
	}
	h.clock.advance(jwksMaxStaleWant + time.Second)
	rec := h.get(time.Second)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("past the bound: status %d headers %v, want 503 with Retry-After, no-store", rec.Code, rec.Header())
	}
	keys.release()
	if !eventually(5*time.Second, func() bool { return h.get(time.Second).Code == http.StatusOK }) {
		t.Fatal("JWKS did not answer 200 after the key store answered again")
	}
}

// A failed read keeps the last good document for at most the bound, then 503.
func TestJWKSReadErrorServesLastGoodUpToTheBoundThen503(t *testing.T) {
	set, _ := twoKeySet(t)
	keys := &scriptedKeys{set: set}
	h := startedHarness(t, keys, 10*time.Millisecond)
	first := h.get(time.Second)
	if first.Code != http.StatusOK {
		t.Fatalf("healthy store: status %d", first.Code)
	}
	keys.serve(token.KeySet{}, errors.New("key file unreadable"))
	failedFrom := keys.calls.Load()
	if !eventually(5*time.Second, func() bool { return keys.calls.Load() >= failedFrom+2 }) {
		t.Log("the refresher did not read the failing store twice")
	}
	h.clock.advance(jwksMaxStaleWant - time.Second)
	inside := h.get(time.Second)
	if inside.Code != http.StatusOK || inside.Body.String() != first.Body.String() || inside.Header().Get("ETag") != first.Header().Get("ETag") {
		t.Fatalf("inside the bound: status %d, want 200 with the last good document", inside.Code)
	}
	h.clock.advance(2 * time.Second)
	if rec := h.get(time.Second); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("past the bound: status %d, want 503", rec.Code)
	}
}

// A compromise latched after the document was built drops it and the
// last-good copy at once: the next request answers 503, never the document
// that publishes the key, and a refresh is started at once.
func TestJWKSCompromiseLatchDropsTheDocumentAtOnce(t *testing.T) {
	first, _ := keysOf(t, [2]string{"auth-old", "retiring"}, [2]string{"auth-bad", "active"})
	keys := &scriptedKeys{set: first}
	h := startedHarness(t, keys, time.Hour)
	if rec := h.get(time.Second); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "auth-bad") {
		t.Fatalf("before the latch: status %d body %s", rec.Code, rec.Body.String())
	}
	keys.latch("auth-bad")
	rec := h.get(time.Second)
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "auth-bad") {
		t.Fatalf("after the latch: status %d body %s, want 503 without the key", rec.Code, rec.Body.String())
	}
	second, _ := keysOf(t, [2]string{"auth-old", "retiring"}, [2]string{"auth-new", "active"})
	keys.serve(second, nil)
	var served *httptest.ResponseRecorder
	if !eventually(5*time.Second, func() bool {
		served = h.get(time.Second)
		return served.Code == http.StatusOK
	}) {
		t.Fatal("no refresh replaced the dropped document (the hourly tick is the only other trigger)")
	}
	if body := served.Body.String(); strings.Contains(body, "auth-bad") || !strings.Contains(body, "auth-new") {
		t.Fatalf("after the refresh: %s", body)
	}
}

// The refresher reads in the background with no request, and stops with the
// server lifecycle, also while its read hangs: no goroutine is left.
func TestJWKSRefresherRunsInTheBackgroundAndStopsWithTheServer(t *testing.T) {
	set, _ := twoKeySet(t)
	keys := &scriptedKeys{set: set}
	before := runtime.NumGoroutine()
	h := newJWKSHarness(t, keys, 5*time.Millisecond)
	h.start(t)
	from := keys.calls.Load()
	if !eventually(5*time.Second, func() bool { return keys.calls.Load() >= from+3 }) {
		t.Fatalf("the refresher read the store %d times with no request, want at least 3", keys.calls.Load()-from)
	}
	keys.hangReads()
	if !eventually(5*time.Second, func() bool { return keys.pending.Load() > 0 }) {
		t.Fatal("no refresh entered the hung read")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if pending := keys.pending.Load(); pending != 0 {
		t.Fatalf("%d reads still hang after stop", pending)
	}
	stopped := keys.calls.Load()
	time.Sleep(50 * time.Millisecond)
	if after := keys.calls.Load(); after != stopped {
		t.Fatalf("the refresher read the store %d times after stop", after-stopped)
	}
	if !eventually(5*time.Second, func() bool { return runtime.NumGoroutine() <= before }) {
		t.Fatalf("goroutines %d after stop, %d before start", runtime.NumGoroutine(), before)
	}
	keys.release()
}
