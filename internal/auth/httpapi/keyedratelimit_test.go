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
// windowLimiter is the window counters plus a count limit: the shape the
// limiter had before the store seam, kept so the window and cardinality
// tests exercise the counters directly.
type windowLimiter struct {
	*windowCounters
	limit int
}

func newWindowLimiter(limit int, window time.Duration, now func() time.Time) *windowLimiter {
	return &windowLimiter{windowCounters: newWindowCounters(window, now), limit: limit}
}

func (w *windowLimiter) Allow(key, path string) bool {
	count, admitted := w.hit(key, path)
	return admitted && count <= w.limit
}

func TestKeyedLimiterFixedWindowRefusesTheNthPlusOneHit(t *testing.T) {
	now := time.Now()
	limiter := newWindowLimiter(5, time.Hour, func() time.Time { return now })

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
	limiter := newWindowLimiter(5, time.Hour, func() time.Time { return now })

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
	limiter := newWindowLimiter(1, time.Hour, func() time.Time { return now })

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
	limiter := newWindowLimiter(5, time.Hour, func() time.Time { return now })

	// 5,000 distinct pairs, spread over five keys.
	const distinctPaths = 5000
	for i := range distinctPaths {
		limiter.Allow(fmt.Sprintf("admin-user:%d", i/1000), fmt.Sprintf("/users/%d/password", i))
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
// created WITHIN one window. Proves the global backstop: however it is
// filled, the map never grows past maxKeyedLimiterEntries, a genuinely NEW pair past
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

// TestKeyedLimiterHasNoPerCallerPathBound is CHAOS-6624: slowapi counts one
// bucket per (caller, exact path) and bounds nothing, so a caller reading
// more distinct paths than any per-caller bound (this limiter had one of
// 1,000, CHAOS-6459) is never refused for the NUMBER of paths. Every one of
// 2,500 distinct paths from one caller is admitted, each on its own budget.
func TestKeyedLimiterHasNoPerCallerPathBound(t *testing.T) {
	now := time.Now()
	limiter := newWindowLimiter(5, time.Hour, func() time.Time { return now })
	for i := range 2500 {
		if !limiter.Allow("admin-user:walker", fmt.Sprintf("/orgs/%d/invites", i)) {
			t.Fatalf("distinct path %d of one caller was refused, want admitted (Python bounds nothing)", i+1)
		}
	}
	// Each path is still its own bucket: the sixth hit on one path is refused.
	for range 4 {
		limiter.Allow("admin-user:walker", "/orgs/0/invites")
	}
	if limiter.Allow("admin-user:walker", "/orgs/0/invites") {
		t.Fatal("the sixth hit on one path was admitted, want refused (5/window)")
	}
}

func TestKeyedLimiterZeroConfigurationIsUnlimited(t *testing.T) {
	store := NewMemoryCounters(nil)
	for _, limit := range []Limit{
		{ID: "t", Count: 0, Window: time.Hour},
		{ID: "t", Count: 5, Window: 0},
		{ID: "", Count: 5, Window: time.Hour},
	} {
		if NewKeyedLimiter(store, limit) != nil {
			t.Fatalf("limit %+v should yield a nil limiter", limit)
		}
	}
	if NewKeyedLimiter(nil, Limit{ID: "t", Count: 5, Window: time.Hour}) != nil {
		t.Fatal("a nil store should yield a nil limiter")
	}
	var nilLimiter *KeyedLimiter
	if ok, err := nilLimiter.Allow(context.Background(), "k", "/p"); !ok || err != nil {
		t.Fatalf("a nil *KeyedLimiter must allow every call, got %v, %v", ok, err)
	}
}

// TestKeyedLimiterIsSafeUnderConcurrency mirrors TestBucketIsSafeUnderConcurrency:
// concurrent callers hitting the SAME (key, path) must never observe more
// than `limit` allowed calls.
func TestKeyedLimiterIsSafeUnderConcurrency(t *testing.T) {
	const limit = 50
	limiter := newWindowLimiter(limit, time.Hour, nil)

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
	store := NewMemoryCounters(func() time.Time { return now })
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
	handler := LimitWith(NewKeyedLimiter(store, limit), keyFunc, write)(inner)

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
	store := NewMemoryCounters(nil)
	limit := Limit{ID: "test", Count: 1, Window: time.Hour}
	keyFunc := func(r *http.Request) string { return "k" }
	handler := LimitWith(NewKeyedLimiter(store, limit), keyFunc, WriteError)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

// CHAOS-6624 through the real middleware: one admin walking 1,500 distinct
// paths (each answered 403 by the handler, as an org-access refusal would
// be) is never answered 429 by the limiter, and every other admin is
// unaffected.
func TestKeyedRateLimitAdminWalkingManyDistinctPathsIsNeverRefusedForTheirNumber(t *testing.T) {
	now := time.Now()
	store := NewMemoryCounters(func() time.Time { return now })
	limit := Limit{ID: "walk_probe", Count: 10, Window: time.Hour}
	handler := LimitWith(NewKeyedLimiter(store, limit), func(r *http.Request) string { return r.Header.Get("X-Admin") }, WriteError)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	call := func(admin, path string) int {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.Header.Set("X-Admin", admin)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code
	}
	for i := range 1500 {
		if got := call("admin-a", fmt.Sprintf("/api/v1/admin/orgs/%d/invites", i)); got != http.StatusForbidden {
			t.Fatalf("distinct path %d = %d, want 403 from the handler (a 429 is the CHAOS-6624 path bound)", i+1, got)
		}
	}
	if got := call("admin-b", "/api/v1/admin/orgs/other/invites"); got != http.StatusForbidden {
		t.Fatalf("another admin's first request = %d, want 403 from the handler", got)
	}
}

const (
	// smallPerKey is only how many paths the saturation tests create per key
	// while filling the set; nothing bounds a key to it.
	smallPerKey = 10
	smallGlobal = 100
)

// smallLimiter is window counters with the global bound shrunk so the
// saturation logic is exercised without 100,000 calls.
func smallLimiter(now *time.Time) *windowLimiter {
	limiter := newWindowLimiter(5, time.Hour, func() time.Time { return *now })
	limiter.globalBound = smallGlobal
	return limiter
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
	store := NewMemoryCounters(func() time.Time { return now })
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
	handler := ValidateThenLimit(validate, NewKeyedLimiter(store, limit), func(*http.Request) string { return "admin" }, WriteError)(
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
	ValidateThenLimit(nil, NewKeyedLimiter(NewMemoryCounters(nil), Limit{ID: "t", Count: 1, Window: time.Hour}), func(*http.Request) string { return "" }, WriteError)
}

// failingStore is a CounterStore whose backend is down.
type failingStore struct{}

func (failingStore) Increment(context.Context, Hit) (int64, error) {
	return 0, errors.New("valkey is down")
}
func (failingStore) Backend() string { return "redis" }
func (failingStore) Peek(context.Context, Hit) (int64, error) {
	return 0, errors.New("valkey is down")
}

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
	handler := LimitWith(NewKeyedLimiter(failingStore{}, Limit{ID: "test", Count: 5, Window: time.Hour}), func(*http.Request) string { return "secret-caller-key" }, write)(
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
func TestMemoryCountersKeepLimitsApart(t *testing.T) {
	now := time.Now()
	store := NewMemoryCounters(func() time.Time { return now })
	a := NewKeyedLimiter(store, Limit{ID: "a", Count: 1, Window: time.Hour})
	b := NewKeyedLimiter(store, Limit{ID: "b", Count: 1, Window: time.Hour})
	ctx := context.Background()
	if ok, _ := a.Allow(ctx, "k", "/p"); !ok {
		t.Fatal("first hit of limit a refused")
	}
	if ok, _ := a.Allow(ctx, "k", "/p"); ok {
		t.Fatal("second hit of limit a allowed")
	}
	if ok, _ := b.Allow(ctx, "k", "/p"); !ok {
		t.Fatal("limit b shares limit a's bucket")
	}
	if store.Backend() != "memory" || a.Backend() != "memory" {
		t.Fatalf("Backend() = %q / %q, want memory", store.Backend(), a.Backend())
	}
}

// A store that reports ErrPathBound refuses without an error (the caller is
// at its bound of distinct paths); any other error is returned.
func TestKeyedLimiterMapsPathBoundToARefusalAndErrorsThrough(t *testing.T) {
	limit := Limit{ID: "t", Count: 5, Window: time.Hour}
	if ok, err := NewKeyedLimiter(boundStore{}, limit).Allow(context.Background(), "k", "/p"); ok || err != nil {
		t.Fatalf("path bound = %v, %v, want refused with nil error", ok, err)
	}
	if ok, err := NewKeyedLimiter(failingStore{}, limit).Allow(context.Background(), "k", "/p"); ok || err == nil {
		t.Fatalf("store error = %v, %v, want refused with the error", ok, err)
	}
}

type boundStore struct{}

func (boundStore) Increment(context.Context, Hit) (int64, error) { return 0, ErrPathBound }
func (boundStore) Backend() string                               { return "test" }
func (boundStore) Peek(context.Context, Hit) (int64, error)      { return 0, nil }

// The store error counter is a metrics source: the series exists at zero as
// soon as a limit is wired, counts each store error by limit id, and never
// carries a caller key or a path.
func TestRateLimitStoreErrorsAreWrittenAsPrometheusSeries(t *testing.T) {
	before := scrapeStoreErrors(t)
	limit := Limit{ID: "metrics_probe", Count: 5, Window: time.Hour}
	handler := LimitWith(NewKeyedLimiter(failingStore{}, limit), func(*http.Request) string { return "secret-caller-key" }, WriteError)(
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

// Scans run only when something can have expired: hammering a saturated map
// with nothing expired costs no scan at all, and the first request after
// expiry costs exactly one.
func TestKeyedLimiterScansOnlyWhenSomethingCanHaveExpired(t *testing.T) {
	now := time.Now()
	limiter := smallLimiter(&now)
	for k := range smallGlobal / smallPerKey {
		for i := range smallPerKey {
			limiter.Allow(fmt.Sprintf("admin-user:%d", k), fmt.Sprintf("/p/%d", i))
		}
	}
	for i := range 200 {
		limiter.Allow(fmt.Sprintf("admin-user:new-%d", i), "/over/x") // map at its cap
	}
	if limiter.fullSweeps != 0 {
		t.Fatalf("scans ran with nothing expired: fullSweeps=%d", limiter.fullSweeps)
	}
	now = now.Add(time.Hour)
	limiter.lastSweep = now // the periodic sweep is not due: the on-demand one must do the work
	limiter.Allow("admin-user:fresh", "/p/x")
	if limiter.fullSweeps != 1 {
		t.Fatalf("fullSweeps after expiry = %d, want exactly 1", limiter.fullSweeps)
	}
}

// Test is slowapi's `limits` test(): true while the stored count is below the
// limit, and it never counts. A pair with no live counter is at zero.
func TestKeyedLimiterTestDoesNotConsumeAndFollowsTheWindow(t *testing.T) {
	now := time.Now()
	store := NewMemoryCounters(func() time.Time { return now })
	limiter := NewKeyedLimiter(store, Limit{ID: "peek", Count: 2, Window: time.Minute})
	ctx := context.Background()
	for range 5 {
		if ok, err := limiter.Test(ctx, "k", ""); !ok || err != nil {
			t.Fatalf("Test on an untouched pair = %v, %v, want true (it must never consume)", ok, err)
		}
	}
	if ok, _ := limiter.Allow(ctx, "k", ""); !ok {
		t.Fatal("the first hit was refused: Test consumed allowance")
	}
	if ok, _ := limiter.Test(ctx, "k", ""); !ok {
		t.Fatal("Test at count 1 of 2 = false, want true")
	}
	limiter.Allow(ctx, "k", "")
	if ok, _ := limiter.Test(ctx, "k", ""); ok {
		t.Fatal("Test at count 2 of 2 = true, want false (count is no longer below the limit)")
	}
	if ok, _ := limiter.Test(ctx, "other", ""); !ok {
		t.Fatal("another key shares the exhausted pair's count")
	}
	now = now.Add(time.Minute)
	if ok, _ := limiter.Test(ctx, "k", ""); !ok {
		t.Fatal("Test after the window = false, want true (a new window)")
	}
	var nilLimiter *KeyedLimiter
	if ok, err := nilLimiter.Test(ctx, "k", ""); !ok || err != nil {
		t.Fatalf("a nil limiter Test = %v, %v, want true", ok, err)
	}
}

func TestKeyedLimiterTestReturnsAStoreError(t *testing.T) {
	limiter := NewKeyedLimiter(failingStore{}, Limit{ID: "peek_err", Count: 2, Window: time.Minute})
	if ok, err := limiter.Test(context.Background(), "k", ""); ok || err == nil {
		t.Fatalf("Test on a failing store = %v, %v, want false with the error", ok, err)
	}
}
