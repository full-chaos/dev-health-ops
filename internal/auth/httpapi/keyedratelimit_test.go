package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestKeyedLimiterFixedWindowRefusesTheNthPlusOneHit is the same-key
// exhaustion case: a single (key, path) pair may make `limit` calls inside
// one window; the next one is refused.
func TestKeyedLimiterFixedWindowRefusesTheNthPlusOneHit(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(5, time.Hour, func() time.Time { return now })

	for attempt := range 5 {
		if !limiter.Allow("admin-user:a", "/x") {
			t.Fatalf("call %d was refused, want allowed (limit is 5)", attempt+1)
		}
	}
	if limiter.Allow("admin-user:a", "/x") {
		t.Fatal("call 6 was allowed, want refused")
	}
}

// TestKeyedLimiterWindowDoesNotRollOverEarly reproduces the codex-review
// finding on #2866 r1: slowapi's fixed-window strategy does not roll over
// just because time has passed within the window -- a 6th request at
// t=12m, inside a "5/hour" window that started at t=0, is still refused.
// Only a request AFTER the full window has elapsed sees a fresh bucket.
func TestKeyedLimiterWindowDoesNotRollOverEarly(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(5, time.Hour, func() time.Time { return now })

	for range 5 {
		if !limiter.Allow("admin-user:a", "/x") {
			t.Fatal("a call inside the initial burst was refused")
		}
	}
	now = now.Add(12 * time.Minute)
	if limiter.Allow("admin-user:a", "/x") {
		t.Fatal("the 6th request at t=12m was allowed, want refused (Python's fixed window is still open until t=60m)")
	}
	now = now.Add(48*time.Minute + time.Second)
	if !limiter.Allow("admin-user:a", "/x") {
		t.Fatal("a request just after the window closed was refused, want allowed (fresh window)")
	}
}

// TestKeyedLimiterIsIndependentPerKeyAndPath covers the review's other
// P1: a shared bucket per route pattern let one admin's resets for one
// target consume another target's (or another admin's) quota. Each
// (key, path) pair gets its own budget.
func TestKeyedLimiterIsIndependentPerKeyAndPath(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(1, time.Hour, func() time.Time { return now })

	if !limiter.Allow("admin-user:a", "/users/target-1/password") {
		t.Fatal("admin a, target 1: first call refused")
	}
	if !limiter.Allow("admin-user:a", "/users/target-2/password") {
		t.Fatal("admin a, target 2 (cross-target): refused by target 1's own exhausted bucket")
	}
	if !limiter.Allow("admin-user:b", "/users/target-1/password") {
		t.Fatal("admin b, target 1 (cross-admin): refused by admin a's own exhausted bucket")
	}
	if limiter.Allow("admin-user:a", "/users/target-1/password") {
		t.Fatal("admin a, target 1 second call was allowed, want refused (its own bucket is exhausted)")
	}
}

// TestKeyedLimiterEvictsExpiredEntries is the codex-review pr2873-r1 P1:
// a caller-supplied path (a target user id in the URL, resolved before the
// handler validates it exists) makes the limiter's real key cardinality
// unbounded by request volume, not by distinct admins x distinct routes as
// an earlier version of this file's doc comment wrongly claimed. Many
// distinct paths must not survive past their own window forever.
func TestKeyedLimiterEvictsExpiredEntries(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(5, time.Hour, func() time.Time { return now })

	const distinctPaths = 5000
	for i := range distinctPaths {
		limiter.Allow("admin-user:a", fmt.Sprintf("/users/%d/password", i))
	}
	limiter.mu.Lock()
	got := len(limiter.entries)
	limiter.mu.Unlock()
	if got != distinctPaths {
		t.Fatalf("entries after %d distinct paths = %d, want %d (all still within their window)", distinctPaths, got, distinctPaths)
	}

	now = now.Add(2 * time.Hour)
	limiter.Allow("admin-user:a", "/users/fresh/password")

	limiter.mu.Lock()
	got = len(limiter.entries)
	limiter.mu.Unlock()
	if got != 1 {
		t.Fatalf("entries after all old windows expired = %d, want 1 (only the fresh path)", got)
	}
}

// TestKeyedLimiterZeroConfigurationIsUnlimited matches NewBucket's own
// degenerate-configuration contract: a non-positive limit or window never
// silently produces a limiter that refuses everything.
func TestKeyedLimiterZeroConfigurationIsUnlimited(t *testing.T) {
	if NewKeyedLimiter(0, time.Hour, nil) != nil {
		t.Fatal("limit=0 should yield a nil limiter")
	}
	if NewKeyedLimiter(5, 0, nil) != nil {
		t.Fatal("window=0 should yield a nil limiter")
	}
	var nilLimiter *KeyedLimiter
	if !nilLimiter.Allow("k", "/p") {
		t.Fatal("a nil *KeyedLimiter must allow every call")
	}
}

// TestKeyedLimiterIsSafeUnderConcurrency mirrors TestBucketIsSafeUnderConcurrency:
// concurrent callers hitting the SAME (key, path) must never observe more
// than `limit` allowed calls.
func TestKeyedLimiterIsSafeUnderConcurrency(t *testing.T) {
	const limit = 50
	limiter := NewKeyedLimiter(limit, time.Hour, nil)

	var allowed int
	var mu sync.Mutex
	var group sync.WaitGroup
	for range 200 {
		group.Add(1)
		go func() {
			defer group.Done()
			if limiter.Allow("k", "/p") {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	group.Wait()
	if allowed != limit {
		t.Fatalf("allowed = %d, want exactly %d", allowed, limit)
	}
}

// TestKeyedRateLimitWithRefusesOverTheKeyedBudget is the middleware-level
// proof: the handler behind it is reached exactly `limit` times per key,
// and a refusal renders through the configured ErrorWriter.
func TestKeyedRateLimitWithRefusesOverTheKeyedBudget(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(2, time.Hour, func() time.Time { return now })
	var reached int
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		w.WriteHeader(http.StatusNoContent)
	})
	var wroteCode Code
	write := func(w http.ResponseWriter, r *http.Request, code Code) {
		wroteCode = code
		w.WriteHeader(http.StatusTooManyRequests)
	}
	keyFunc := func(r *http.Request) string { return r.Header.Get("X-Test-Admin") }
	handler := KeyedRateLimitWith(limiter, keyFunc, write)(inner)

	call := func(admin string) int {
		request := httptest.NewRequest(http.MethodPost, "/x", nil)
		request.Header.Set("X-Test-Admin", admin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}

	if got := call("a"); got != http.StatusNoContent {
		t.Fatalf("admin a call 1 = %d, want 204", got)
	}
	if got := call("a"); got != http.StatusNoContent {
		t.Fatalf("admin a call 2 = %d, want 204", got)
	}
	if got := call("a"); got != http.StatusTooManyRequests {
		t.Fatalf("admin a call 3 = %d, want 429", got)
	}
	if wroteCode != CodeRateLimited {
		t.Fatalf("refusal code = %q, want %q", wroteCode, CodeRateLimited)
	}
	if got := call("b"); got != http.StatusNoContent {
		t.Fatalf("admin b (independent budget) = %d, want 204", got)
	}
	if reached != 3 {
		t.Fatalf("inner handler reached %d times, want 3 (2 for a, 1 for b)", reached)
	}
}
