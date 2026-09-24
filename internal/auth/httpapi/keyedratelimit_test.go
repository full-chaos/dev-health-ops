package httpapi

import (
	"context"
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

// TestKeyedLimiterCapsMapGrowthWithinOneWindow is the codex-review
// pr2873-r2 P1: sweep only reclaims an entry once ITS OWN window has
// elapsed, so it cannot bound how many distinct (key, path) pairs a
// caller creates WITHIN one window -- reproduced live with 5,000 distinct
// paths from one caller, all still present, no sweep yet due. Proves the
// hard cap: the map never grows past maxKeyedLimiterEntries, and once
// saturated a genuinely NEW pair is refused (fail closed), while an
// already-tracked pair's own counting is unaffected by the cap.
func TestKeyedLimiterCapsMapGrowthWithinOneWindow(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(5, time.Hour, func() time.Time { return now })

	for i := range maxKeyedLimiterEntries {
		if !limiter.Allow("admin-user:a", fmt.Sprintf("/x/%d", i)) {
			t.Fatalf("path %d (within the cap) was refused, want allowed", i)
		}
	}
	limiter.mu.Lock()
	got := len(limiter.entries)
	limiter.mu.Unlock()
	if got != maxKeyedLimiterEntries {
		t.Fatalf("entries after filling the cap = %d, want %d", got, maxKeyedLimiterEntries)
	}

	if limiter.Allow("admin-user:a", "/x/one-too-many") {
		t.Fatal("a brand-new pair past the cap was allowed, want refused (fail closed)")
	}
	limiter.mu.Lock()
	got = len(limiter.entries)
	limiter.mu.Unlock()
	if got != maxKeyedLimiterEntries {
		t.Fatalf("entries after the refused pair = %d, want unchanged at %d (the cap must never grow the map)", got, maxKeyedLimiterEntries)
	}

	// An already-tracked pair keeps working normally even while the map
	// is saturated -- the cap only refuses ADMISSION of a new pair.
	if !limiter.Allow("admin-user:a", "/x/0") {
		t.Fatal("an existing, already-counted pair was refused by the saturation cap, want allowed (it is not a new pair)")
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

// TestKeyedRateLimitUsesTheDefaultErrorWriter exercises KeyedRateLimit
// itself (not just KeyedRateLimitWith, which the mounted admin route
// uses) -- codex-review pr2873-r2 P3: KeyedRateLimit had 0% coverage.
// Proves its refusal renders through this package's default WriteError,
// the Python-wire generic envelope (Code, not a caller-supplied writer).
func TestKeyedRateLimitUsesTheDefaultErrorWriter(t *testing.T) {
	limiter := NewKeyedLimiter(1, time.Hour, nil)
	keyFunc := func(r *http.Request) string { return "k" }
	handler := KeyedRateLimit(limiter, keyFunc)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	call := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/x", nil))
		return response
	}

	if got := call().Code; got != http.StatusNoContent {
		t.Fatalf("call 1 = %d, want 204", got)
	}
	second := call()
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("call 2 = %d, want 429", second.Code)
	}
	if body := second.Body.String(); body == "" {
		t.Fatal("KeyedRateLimit's default writer produced an empty body")
	}
}

// TestValidateThenLimitSpendsNoAllowanceOnAFailedValidation is the class
// property behind CHAOS-6435: requests the validator refuses cost nothing;
// only requests that pass it count, and the limit still holds for those.
func TestValidateThenLimitSpendsNoAllowanceOnAFailedValidation(t *testing.T) {
	now := time.Now()
	limiter := NewKeyedLimiter(2, time.Hour, func() time.Time { return now })
	type marker struct{}
	validate := func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
		if r.Header.Get("X-Valid") != "yes" {
			http.Error(w, "invalid", http.StatusUnprocessableEntity)
			return nil, false
		}
		return r.WithContext(context.WithValue(r.Context(), marker{}, "validated")), true
	}
	reached := 0
	handler := ValidateThenLimit(validate, limiter, func(*http.Request) string { return "admin" }, WriteError)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached++
			if r.Context().Value(marker{}) != "validated" {
				t.Error("the validator's request did not reach the handler")
			}
			w.WriteHeader(http.StatusOK)
		}))
	call := func(valid bool) int {
		request := httptest.NewRequest(http.MethodPost, "/route", nil)
		if valid {
			request.Header.Set("X-Valid", "yes")
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code
	}

	for i := range 10 {
		if got := call(false); got != http.StatusUnprocessableEntity {
			t.Fatalf("invalid call %d = %d, want 422", i+1, got)
		}
	}
	for i := range 2 {
		if got := call(true); got != http.StatusOK {
			t.Fatalf("valid call %d after ten invalid ones = %d, want 200 (invalid calls must cost nothing)", i+1, got)
		}
	}
	if got := call(true); got != http.StatusTooManyRequests {
		t.Fatalf("third valid call = %d, want 429 (the limit still holds for valid calls)", got)
	}
	if got := call(false); got != http.StatusUnprocessableEntity {
		t.Fatalf("an invalid call once the allowance is spent = %d, want 422 (validation answers before the limiter)", got)
	}
	if reached != 2 {
		t.Fatalf("handler reached %d times, want 2", reached)
	}
}

func TestValidateThenLimitRequiresAValidator(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a nil validator was accepted; it would limit first and silently reintroduce the divergence")
		}
	}()
	ValidateThenLimit(nil, NewKeyedLimiter(1, time.Hour, nil), func(*http.Request) string { return "" }, WriteError)
}
