package httpapi

import (
	"net/http"
	"sync"
	"time"
)

// KeyFunc derives a rate-limit key from a request -- typically an
// authenticated caller's identity, resolved by middleware that ran before
// this one (e.g. policy.Guard.Wrap has already set the principal on the
// request's context by the time a KeyedLimiter wraps the guarded handler).
type KeyFunc func(*http.Request) string

// fixedWindowEntry is one (key, path) pair's current window: when it
// started and how many hits it has counted so far, including refused ones.
type fixedWindowEntry struct {
	start time.Time
	count int
}

// maxKeyedLimiterEntries hard-caps the in-process counter set's live entry
// count. It exists only because this store lives in process memory: the
// shared store (ratelimitvalkey) bounds nothing, its counters expire by TTL,
// and slowapi bounds nothing either. sweep only reclaims an entry once its
// OWN window has fully elapsed, so it cannot bound how many pairs a caller
// creates within one window; without a cap, one authenticated caller hitting
// a route whose path carries a caller-chosen id could grow this map without
// limit inside a window (reproduced live: 5,000 distinct paths from one
// caller, all present, sweep not yet due). Once the cap is reached a
// BRAND-NEW (key, path) pair is refused outright rather than admitted -- fail
// closed, never fail open -- and an already-tracked pair is never affected.
//
// This is a NAMED, MEMORY-STORE-ONLY divergence from Python: the api refuses
// to start outside development without the shared store, so a deployment
// never counts here. There is deliberately NO per-caller bound: a per-caller
// cap on distinct paths refused a legitimate client Python serves (its 1,001st
// distinct path in a window, CHAOS-6624), the same defect the shared store had.
// 100,000 is far above any real caller population x real limited-path
// count, and far below what would pressure process memory (a
// fixedWindowEntry plus its map overhead is on the order of 100 bytes).
const maxKeyedLimiterEntries = 100_000

// windowCounters is the in-process fixed-window counter set, scoped per (key,
// exact request path) -- the Go equivalent of Python's slowapi default
// strategy (the `limits` package's FixedWindowRateLimiter over
// MemoryStorage). Confirmed live against a real slowapi-backed FastAPI route
// (CHAOS-6357's PR body carries the executed proof): a window starts on the
// caller's first hit after the previous window expired and lasts exactly
// `window` from that moment -- NOT epoch-aligned, and NOT reset early by a
// refused hit. A caller's 6th request inside a "5/hour" window, even at
// t=12m, is still refused; the window only rolls over once `window` has
// elapsed since it started. Two different (key, path) pairs never share a
// bucket: the same admin hitting two different target paths, or two
// different admins hitting the same path, each get their own independent
// budget.
type windowCounters struct {
	window time.Duration
	now    func() time.Time

	mu sync.Mutex
	// entries' key includes the exact request PATH, which for a route like
	// /users/{user_id}/password carries a caller-supplied target id -- NOT a
	// small fixed route set. sweep (below) evicts every expired entry at
	// most once per window, bounding steady-state memory to callers active
	// within the last window; it CANNOT bound growth from many distinct pairs
	// created within a single window, since none of them have expired yet --
	// globalBound is the hard backstop for that case.
	entries map[string]*fixedWindowEntry
	// globalBound is maxKeyedLimiterEntries; a field so a test can exercise
	// the same logic at a small size.
	globalBound int
	// globalNextExpiry is a LOWER BOUND on when the earliest entry can
	// expire; the zero time means "unknown, scan". A forced sweep runs only
	// once now has reached the bound, i.e. only when something HAS expired,
	// so it always frees at least one entry and never runs pointlessly --
	// and, unlike a time-based cooldown, it can never be skipped while an
	// expired entry is still holding the cap. A rollover only moves an
	// entry's expiry later, so a stored bound is never later than the truth.
	globalNextExpiry time.Time
	// fullSweeps counts the forced passes, for tests.
	fullSweeps int
	// lastSweep is when entries was last swept for expired windows.
	lastSweep time.Time
}

// newWindowCounters returns counters for one limit's window. A non-positive
// window yields nil; callers never hit a nil set (the KeyedLimiter treats a
// degenerate limit as "no limit" before it reaches a store).
func newWindowCounters(window time.Duration, now func() time.Time) *windowCounters {
	if window <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &windowCounters{window: window, now: now, entries: map[string]*fixedWindowEntry{}, globalBound: maxKeyedLimiterEntries}
}

// hit counts one hit for the (key, path) pair in its window and returns the
// window's total so far. A refused hit still counts -- slowapi/limits' own
// atomic increment-then-compare -- and never starts a new window early.
// admitted is false only when the pair is NEW and the set is at its cap:
// nothing is counted then.
func (l *windowCounters) hit(key, path string) (count int, admitted bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.sweep(now)
	entryKey := key + "\x00" + path
	entry := l.entries[entryKey]
	if entry == nil || now.Sub(entry.start) >= l.window {
		if entry == nil {
			// The set is saturated and this pair has never been seen: fail
			// closed rather than grow past the cap -- after a full sweep of
			// expired entries, so stale entries do not hold the cap either.
			// An existing pair (a window rollover) is never refused this way.
			if len(l.entries) >= l.globalBound {
				l.forceSweep(now)
				if len(l.entries) >= l.globalBound {
					return 0, false
				}
			}
			if len(l.entries) == 0 {
				l.globalNextExpiry = now.Add(l.window)
			}
		}
		entry = &fixedWindowEntry{start: now}
		l.entries[entryKey] = entry
	}
	entry.count++
	return entry.count, true
}

// peek returns the window's count for the (key, path) pair without counting
// a hit or changing anything: 0 when the pair has no entry or its window has
// elapsed.
func (l *windowCounters) peek(key, path string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.entries[key+"\x00"+path]
	if entry == nil || l.now().Sub(entry.start) >= l.window {
		return 0
	}
	return entry.count
}

// sweep deletes every entry whose window has fully expired, at most once
// per l.window of wall-clock time -- called with l.mu already held. This
// bounds the sweep's own O(n) cost to once per window rather than every
// call, while guaranteeing an entry outlives its window by at most one
// window's length before it is reclaimed.
func (l *windowCounters) sweep(now time.Time) {
	if !l.lastSweep.IsZero() && now.Sub(l.lastSweep) < l.window {
		return
	}
	l.sweepAll(now)
}

// sweepAll deletes every expired entry and recomputes the next-expiry bound
// exactly.
func (l *windowCounters) sweepAll(now time.Time) {
	l.lastSweep = now
	var next time.Time
	for entryKey, entry := range l.entries {
		if now.Sub(entry.start) >= l.window {
			delete(l.entries, entryKey)
			continue
		}
		if expiry := entry.start.Add(l.window); next.IsZero() || expiry.Before(next) {
			next = expiry
		}
	}
	l.globalNextExpiry = next
}

// forceSweep is sweepAll for a saturated map, run only once something can
// have expired.
func (l *windowCounters) forceSweep(now time.Time) {
	if !l.globalNextExpiry.IsZero() && now.Before(l.globalNextExpiry) {
		return
	}
	l.fullSweeps++
	l.sweepAll(now)
}

// RequestValidator is a route's request validation: the checks FastAPI runs
// BEFORE it calls the endpoint (a pydantic body model's field constraints).
// On failure it writes the response itself and returns false; on success it
// returns the request to hand on, which may carry the parsed values in its
// context so the handler does not parse twice.
type RequestValidator func(w http.ResponseWriter, r *http.Request) (*http.Request, bool)

// ValidateThenLimit is the one implementation of the order every
// Python-limited route with a body model has: validate, THEN spend allowance,
// then run the handler. On the Python api, FastAPI validates the body before
// the endpoint runs and slowapi's @limiter.limit wraps the endpoint, so a
// request that fails validation is a 422 that costs the caller nothing
// against the limit. Validating inside the handler, behind LimitWith,
// spends one allowance per malformed request instead -- measured against the
// live Python api on set_user_password: five malformed bodies then six valid
// ones is 200 x5 then 429 on Python, and 429 on every valid call on the old
// Go wiring.
//
// Only the pydantic-level validation belongs in validate. What Python does
// INSIDE the endpoint (password-policy checks, org-access checks) runs after
// the limiter there and must stay in the handler, where it keeps counting.
//
// validate is required: a route with no validation stage has no reason to
// use this wrapper (use LimitWith), and a nil validator that
// silently limited first would reintroduce exactly the divergence this
// exists to end.
func ValidateThenLimit(validate RequestValidator, limiter *KeyedLimiter, keyFunc KeyFunc, write ErrorWriter) func(http.Handler) http.Handler {
	if validate == nil {
		panic("httpapi: ValidateThenLimit needs a validator")
	}
	limitWith := LimitWith(limiter, keyFunc, write)
	return func(next http.Handler) http.Handler {
		limited := limitWith(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			checked, ok := validate(w, r)
			if !ok {
				return
			}
			limited.ServeHTTP(w, checked)
		})
	}
}
