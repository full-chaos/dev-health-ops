package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

	// 5,000 distinct pairs, spread over five keys so each stays inside its
	// own per-key bound.
	const distinctPaths = 5000
	for i := range distinctPaths {
		limiter.Allow(fmt.Sprintf("admin-user:%d", i/maxKeyedLimiterEntriesPerKey), fmt.Sprintf("/users/%d/password", i))
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
// elapsed, so it cannot bound how many distinct (key, path) pairs are
// created WITHIN one window. Proves the global backstop: filled by as many
// keys as it takes (each key is itself bounded, see the per-key test below),
// the map never grows past maxKeyedLimiterEntries, a genuinely NEW pair past
// it is refused (fail closed), and an already-tracked pair's own counting is
// unaffected.
func TestKeyedLimiterCapsMapGrowthWithinOneWindow(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)

	keys := smallGlobal / smallPerKey
	for k := range keys {
		for i := range smallPerKey {
			if !limiter.Allow(fmt.Sprintf("admin-user:%d", k), fmt.Sprintf("/x/%d", i)) {
				t.Fatalf("key %d path %d (within both caps) was refused, want allowed", k, i)
			}
		}
	}
	limiter.mu.Lock()
	got := len(limiter.entries)
	limiter.mu.Unlock()
	if got != smallGlobal {
		t.Fatalf("entries after filling the cap = %d, want %d", got, smallGlobal)
	}

	if limiter.Allow("admin-user:fresh", "/x/one-too-many") {
		t.Fatal("a brand-new pair past the global cap was allowed, want refused (fail closed)")
	}
	limiter.mu.Lock()
	got = len(limiter.entries)
	limiter.mu.Unlock()
	if got != smallGlobal {
		t.Fatalf("entries after the refused pair = %d, want unchanged at %d (the cap must never grow the map)", got, smallGlobal)
	}

	// An already-tracked pair keeps working normally even while the map
	// is saturated -- the cap only refuses ADMISSION of a new pair.
	if !limiter.Allow("admin-user:0", "/x/0") {
		t.Fatal("an existing, already-counted pair was refused by the saturation cap, want allowed (it is not a new pair)")
	}
}

// TestKeyedLimiterOneKeyCannotStarveAnother is CHAOS-6459: one caller minting
// distinct paths used to fill the global cap and turn every other caller's
// first request into a 429. Now the minter is bounded by its own per-key
// cap: its new paths are refused and no other key is affected, however many
// paths it tries.
func TestKeyedLimiterOneKeyCannotStarveAnother(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)

	admitted := 0
	for i := range smallGlobal + 1 {
		if limiter.Allow("admin-user:minter", fmt.Sprintf("/orgs/%d/invites", i)) {
			admitted++
		}
	}
	if admitted != smallPerKey {
		t.Fatalf("the minter got %d distinct paths admitted, want its per-key bound %d", admitted, smallPerKey)
	}
	if !limiter.Allow("admin-user:other", "/orgs/some-org/invites") {
		t.Fatal("another admin's first request was refused after the minter filled ITS OWN cap (the CHAOS-6459 lockout)")
	}
	limiter.mu.Lock()
	minter, total := len(limiter.keyPaths["admin-user:minter"]), len(limiter.entries)
	limiter.mu.Unlock()
	if minter != smallPerKey || total != smallPerKey+1 {
		t.Fatalf("minter holds %d entries, total %d; want %d and %d", minter, total, smallPerKey, smallPerKey+1)
	}
	// The minter's own admitted paths still count normally.
	if !limiter.Allow("admin-user:minter", "/orgs/0/invites") {
		t.Fatal("an already-tracked path of the minter was refused")
	}
}

// Entries leave the per-key count when their window expires, so a key that
// filled its bound gets its allowance back with the next window.
func TestKeyedLimiterPerKeyCountFollowsSweep(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	for i := range smallPerKey {
		limiter.Allow("admin-user:a", fmt.Sprintf("/p/%d", i))
	}
	if limiter.Allow("admin-user:a", "/p/new") {
		t.Fatal("a new path past the per-key bound was admitted")
	}
	now = now.Add(time.Hour + time.Second)
	if !limiter.Allow("admin-user:a", "/p/new") {
		t.Fatal("after the window the key's entries expired but a new path is still refused")
	}
	limiter.mu.Lock()
	got := len(limiter.keyPaths["admin-user:a"])
	limiter.mu.Unlock()
	if got != 1 {
		t.Fatalf("per-key count after the sweep = %d, want 1 (the one new entry)", got)
	}
}

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

// TestLimitWithRefusesOverTheKeyedBudget is the middleware-level
// proof: the handler behind it is reached exactly `limit` times per key,
// and a refusal renders through the configured ErrorWriter.
func TestLimitWithRefusesOverTheKeyedBudget(t *testing.T) {
	now := time.Now()
	store := NewMemoryStore(func() time.Time { return now })
	limit := Limit{ID: "test", Count: 2, Window: time.Hour}
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
	handler := LimitWith(store, limit, keyFunc, write)(inner)

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

// TestLimitWithRendersThroughTheGivenErrorWriter proves a refusal renders
// through the ErrorWriter the caller passes (here this package's own).
func TestLimitWithRendersThroughTheGivenErrorWriter(t *testing.T) {
	store := NewMemoryStore(nil)
	limit := Limit{ID: "test", Count: 1, Window: time.Hour}
	keyFunc := func(r *http.Request) string { return "k" }
	handler := LimitWith(store, limit, keyFunc, WriteError)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		t.Fatal("the error writer produced an empty body")
	}
}

// The round's repro through the real middleware: one admin sends requests on
// 100,000 distinct paths (each answered 403 by the handler, as an org-access
// refusal would be), then a different admin's first request on a fresh path
// must reach the handler, not be refused by the limiter.
func TestKeyedRateLimitFreshAdminIsNotLockedOutByAnotherAdminsPaths(t *testing.T) {
	now := time.Now()
	store := NewMemoryStore(func() time.Time { return now })
	limit := Limit{ID: "cap_probe", Count: 10, Window: time.Hour}
	handler := LimitWith(store, limit, func(r *http.Request) string { return r.Header.Get("X-Admin") }, WriteError)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	call := func(admin, path string) int {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.Header.Set("X-Admin", admin)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code
	}
	// The 100,000 requests share this store; feeding it directly keeps the
	// test fast under the race detector (the middleware's own wiring is what
	// the final request below exercises).
	for i := range 100_000 {
		if _, err := store.Hit(context.Background(), limit, "admin-a", fmt.Sprintf("/api/v1/admin/orgs/%d/invites", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := call("admin-a", "/api/v1/admin/orgs/0/invites"); got != http.StatusForbidden {
		t.Fatalf("admin a's own tracked path through the middleware = %d, want 403 from the handler", got)
	}
	if got := call("admin-b", "/api/v1/admin/orgs/other/invites"); got != http.StatusForbidden {
		t.Fatalf("a fresh admin's first request = %d, want 403 from the handler (a 429 means another admin's paths locked it out)", got)
	}
}

const (
	smallPerKey = 10
	smallGlobal = 100
)

// smallLimiter is a KeyedLimiter with the per-key and global bounds shrunk so
// the saturation logic is exercised without 100,000 calls.
func smallLimiter(now *time.Time) *KeyedLimiter {
	limiter := NewKeyedLimiter(5, time.Hour, func() time.Time { return *now })
	limiter.perKeyBound, limiter.globalBound = smallPerKey, smallGlobal
	return limiter
}

// The review round's repro at the limiter: entries created at staggered times
// must not keep a key locked out after they expire just because the periodic
// full sweep is not due yet. A sweep happens at t=0, the key fills its bound
// from t=1m, another sweep at t=60m+1s finds nothing expired, and at t=61m+1s
// all of the key's entries have expired but no sweep is due until t=120m+1s:
// a new path must be admitted.
func TestKeyedLimiterPerKeyBoundIgnoresExpiredButUnsweptEntries(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	limiter.Allow("marker", "/m") // first call: the periodic sweep runs here
	now = now.Add(time.Minute)
	for i := range smallPerKey {
		limiter.Allow("admin-user:a", fmt.Sprintf("/p/%d", i))
	}
	if limiter.Allow("admin-user:a", "/p/new") {
		t.Fatal("a new path past the per-key bound was admitted")
	}
	// A periodic sweep at t=60m+1s (a different key's request) finds nothing
	// expired yet -- a's entries started at t=1m -- so the next sweep is not
	// due until t=120m+1s.
	now = now.Add(59*time.Minute + time.Second)
	limiter.Allow("marker-2", "/m")
	now = now.Add(time.Minute) // t=61m+1s: a's entries have expired; no sweep is due
	if !limiter.Allow("admin-user:a", "/p/new") {
		t.Fatal("the key is still locked out after all its entries expired (stale entries held its allowance)")
	}
}

// The same at the global backstop: expired-but-unswept entries must not keep
// a brand-new key out.
func TestKeyedLimiterGlobalCapIgnoresExpiredButUnsweptEntries(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	limiter.Allow("marker", "/m")
	now = now.Add(time.Minute)
	for k := range smallGlobal / smallPerKey {
		for i := range smallPerKey {
			limiter.Allow(fmt.Sprintf("admin-user:%d", k), fmt.Sprintf("/p/%d", i))
		}
	}
	if limiter.Allow("admin-user:fresh", "/p/x") {
		t.Fatal("a new pair past the global cap was admitted")
	}
	now = now.Add(59*time.Minute + time.Second)
	limiter.Allow("marker-2", "/m") // periodic sweep: nothing expired yet
	now = now.Add(time.Minute)      // every entry has now expired; no sweep is due
	if !limiter.Allow("admin-user:fresh", "/p/x") {
		t.Fatal("a new key is locked out after every entry expired (stale entries held the global cap)")
	}
}

// TestValidateThenLimitSpendsNoAllowanceOnAFailedValidation is the class
// property behind CHAOS-6435: requests the validator refuses cost nothing;
// only requests that pass it count, and the limit still holds for those.
func TestValidateThenLimitSpendsNoAllowanceOnAFailedValidation(t *testing.T) {
	now := time.Now()
	store := NewMemoryStore(func() time.Time { return now })
	limit := Limit{ID: "test", Count: 2, Window: time.Hour}
	type marker struct{}
	validate := func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
		if r.Header.Get("X-Valid") != "yes" {
			http.Error(w, "invalid", http.StatusUnprocessableEntity)
			return nil, false
		}
		return r.WithContext(context.WithValue(r.Context(), marker{}, "validated")), true
	}
	reached := 0
	handler := ValidateThenLimit(validate, store, limit, func(*http.Request) string { return "admin" }, WriteError)(
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
	ValidateThenLimit(nil, NewMemoryStore(nil), Limit{ID: "t", Count: 1, Window: time.Hour}, func(*http.Request) string { return "" }, WriteError)
}

// failingStore is a HitStore whose backend is down.
type failingStore struct{}

func (failingStore) Hit(context.Context, Limit, string, string) (bool, error) {
	return false, errors.New("valkey is down")
}
func (failingStore) Backend() string { return "redis" }

// A store error is the Python api's unhandled-error 500 (slowapi has no
// swallow_errors): the request is never let through, the handler is never
// reached, and the body carries no detail of the failure.
func TestLimitWithFailsClosedWithA500WhenTheStoreErrors(t *testing.T) {
	reached := false
	var code Code
	write := func(w http.ResponseWriter, r *http.Request, c Code) {
		code = c
		w.WriteHeader(http.StatusInternalServerError)
	}
	handler := LimitWith(failingStore{}, Limit{ID: "test", Count: 5, Window: time.Hour}, func(*http.Request) string { return "secret-caller-key" }, write)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/x", nil))
	if recorder.Code != http.StatusInternalServerError || code != CodeInternal {
		t.Fatalf("store error answered %d (%q), want 500 internal_error", recorder.Code, code)
	}
	if reached {
		t.Fatal("the handler ran although the limit store could not answer")
	}
}

// MemoryStore keeps one limiter per limit ID: two limits never share a
// bucket, and the same limit ID always resolves to the same counters.
func TestMemoryStoreKeepsLimitsApart(t *testing.T) {
	now := time.Now()
	store := NewMemoryStore(func() time.Time { return now })
	a := Limit{ID: "a", Count: 1, Window: time.Hour}
	b := Limit{ID: "b", Count: 1, Window: time.Hour}
	if ok, _ := store.Hit(context.Background(), a, "k", "/p"); !ok {
		t.Fatal("first hit of limit a refused")
	}
	if ok, _ := store.Hit(context.Background(), a, "k", "/p"); ok {
		t.Fatal("second hit of limit a allowed")
	}
	if ok, _ := store.Hit(context.Background(), b, "k", "/p"); !ok {
		t.Fatal("limit b shares limit a's bucket")
	}
	if store.Backend() != "memory" {
		t.Fatalf("Backend() = %q, want memory", store.Backend())
	}
}

// The store error counter is a metrics source: the series exists at zero as
// soon as a limit is wired, counts each store error by limit id, and never
// carries a caller key or a path.
func TestRateLimitStoreErrorsAreWrittenAsPrometheusSeries(t *testing.T) {
	before := scrapeStoreErrors(t)
	limit := Limit{ID: "metrics_probe", Count: 5, Window: time.Hour}
	handler := LimitWith(failingStore{}, limit, func(*http.Request) string { return "secret-caller-key" }, WriteError)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	if got := scrapeStoreErrors(t); !strings.Contains(got, `dev_health_api_rate_limit_store_errors_total{limit="metrics_probe"} 0`) {
		t.Fatalf("a wired limit has no zero series:\n%s", got)
	}
	for range 3 {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/orgs/secret-path/invites", nil))
	}
	after := scrapeStoreErrors(t)
	if !strings.Contains(after, `dev_health_api_rate_limit_store_errors_total{limit="metrics_probe"} 3`) {
		t.Fatalf("three store errors were not counted:\n%s", after)
	}
	if !strings.Contains(after, "# TYPE dev_health_api_rate_limit_store_errors_total counter") {
		t.Fatalf("the series has no TYPE line:\n%s", after)
	}
	if strings.Contains(after, "secret-caller-key") || strings.Contains(after, "secret-path") {
		t.Fatalf("the metric carries a caller key or a path:\n%s", after)
	}
	_ = before
}

func scrapeStoreErrors(t *testing.T) string {
	t.Helper()
	var out strings.Builder
	if err := RateLimitStoreErrors.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// The second review round's repro: a sweep that ran shortly BEFORE the
// entries expired must not make the next request wait out a cooldown.
// Saturate the global cap, let another key's request at t=window-500ms hit
// the saturated map (nothing has expired yet, so no sweep can help), then
// ask for a fresh pair the instant the entries expire: it must be admitted.
func TestKeyedLimiterGlobalCapNeverWaitsOutACooldownAfterExpiry(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	for k := range smallGlobal / smallPerKey {
		for i := range smallPerKey {
			limiter.Allow(fmt.Sprintf("admin-user:%d", k), fmt.Sprintf("/p/%d", i))
		}
	}
	now = now.Add(time.Hour - 500*time.Millisecond)
	limiter.lastSweep = now // a periodic sweep just ran: none is due when the entries expire
	if limiter.Allow("admin-user:fresh", "/p/x") {
		t.Fatal("a new pair past the global cap was admitted while every entry was still live")
	}
	now = now.Add(500 * time.Millisecond) // every entry has just expired
	if !limiter.Allow("admin-user:fresh", "/p/x") {
		t.Fatal("a fresh pair was refused the instant every entry expired (a scan cooldown held the cap)")
	}
}

// The same for one key at its bound.
func TestKeyedLimiterKeyBoundNeverWaitsOutACooldownAfterExpiry(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	for i := range smallPerKey {
		limiter.Allow("admin-user:a", fmt.Sprintf("/p/%d", i))
	}
	now = now.Add(time.Hour - 500*time.Millisecond)
	limiter.lastSweep = now // a periodic sweep just ran: none is due when the entries expire
	if limiter.Allow("admin-user:a", "/p/x") {
		t.Fatal("a new path past the key's bound was admitted while all its entries were live")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.Allow("admin-user:a", "/p/x") {
		t.Fatal("a new path was refused the instant the key's entries expired (a scan cooldown held the bound)")
	}
}

// Scans run only when something can have expired: hammering a saturated key
// or map with nothing expired costs no scan at all, and the first request
// after expiry costs exactly one.
func TestKeyedLimiterScansOnlyWhenSomethingCanHaveExpired(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	for k := range smallGlobal / smallPerKey {
		for i := range smallPerKey {
			limiter.Allow(fmt.Sprintf("admin-user:%d", k), fmt.Sprintf("/p/%d", i))
		}
	}
	for i := range 200 {
		limiter.Allow("admin-user:0", fmt.Sprintf("/over/%d", i))     // key at its bound
		limiter.Allow(fmt.Sprintf("admin-user:new-%d", i), "/over/x") // map at its cap
	}
	if limiter.keyScans != 0 || limiter.fullSweeps != 0 {
		t.Fatalf("scans ran with nothing expired: keyScans=%d fullSweeps=%d", limiter.keyScans, limiter.fullSweeps)
	}
	now = now.Add(time.Hour)
	limiter.lastSweep = now // the periodic sweep is not due: the on-demand one must do the work
	limiter.Allow("admin-user:fresh", "/p/x")
	if limiter.fullSweeps != 1 {
		t.Fatalf("fullSweeps after expiry = %d, want exactly 1", limiter.fullSweeps)
	}
}
