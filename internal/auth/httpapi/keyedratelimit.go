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

// maxKeyedLimiterEntries hard-caps a KeyedLimiter's live entry count. sweep
// only reclaims an entry once its OWN window has fully elapsed -- it has no
// power to shrink the set of entries still inside their current window, so
// it cannot bound how many an authenticated caller creates within a single
// window by hitting many distinct paths before that window ever expires
// (reproduced live: 5,000 distinct paths from one caller, all still
// present, sweep not yet due). This cap is the backstop for exactly that
// window: once reached, a BRAND-NEW (key, path) pair is refused outright
// rather than admitted, so the map can never grow past it -- fail closed
// on cardinality, never fail open. It never affects an already-tracked
// pair's own counting. 100,000 is far above any real admin population x
// real rate-limited admin path count (see KeyedLimiter's own doc comment
// on that expected cardinality), and far below what would pressure
// process memory (a fixedWindowEntry plus its map overhead is on the
// order of 100 bytes).
const maxKeyedLimiterEntries = 100_000

// KeyedLimiter is a fixed-window rate limiter scoped per (key, exact request
// path) -- the Go equivalent of Python's slowapi default strategy (the
// `limits` package's FixedWindowRateLimiter over MemoryStorage). Confirmed
// live against a real slowapi-backed FastAPI route (CHAOS-6357's PR body
// carries the executed proof): a window starts on the caller's first hit
// after the previous window expired and lasts exactly `window` from that
// moment -- NOT epoch-aligned, and NOT reset early by a refused hit. A
// caller's 6th request inside a "5/hour" window, even at t=12m, is still
// refused; the window only rolls over once `window` has elapsed since it
// started. Two different (key, path) pairs never share a bucket: the same
// admin hitting two different target paths, or two different admins
// hitting the same path, each get their own independent budget.
//
// Unlike Bucket (this package's per-ROUTE, unauthenticated token bucket --
// see its own doc comment: "Per-caller quota ... needs an authenticated
// principal, which this dormant wave does not have"), KeyedLimiter IS that
// later wave: it needs an authenticated principal for its key, so it is
// applied INSIDE a route's handler chain, after authentication has already
// resolved (wrapped around the handler a Guard passes to its own `next`),
// never at the mux level routeChain wraps every route with -- routeChain
// runs before any route-specific authentication.
type KeyedLimiter struct {
	limit  int
	window time.Duration
	now    func() time.Time

	mu sync.Mutex
	// entries' key includes the exact request PATH, which for a route
	// like /users/{user_id}/password carries a caller-supplied target id
	// -- NOT a small fixed route set. An authenticated caller sending
	// distinct target ids (the handler validates the target only AFTER
	// this limiter runs) can otherwise grow this map. sweep (below)
	// evicts every expired entry at most once per window, bounding
	// steady-state memory to callers active within the last window; it
	// CANNOT bound growth from many distinct pairs created within a
	// single window, since none of them have expired yet for sweep to
	// reclaim -- maxKeyedLimiterEntries is the hard backstop for that
	// case (reproduced live: 5,000 distinct paths inside one window, all
	// still present, no sweep yet due).
	entries map[string]*fixedWindowEntry
	// lastSweep is when entries was last swept for expired windows.
	lastSweep time.Time
}

// NewKeyedLimiter returns a limiter allowing at most limit hits per window,
// per (key, path). A non-positive limit or window yields a nil limiter,
// which Allow treats as "no limit" -- the same degenerate-configuration
// contract NewBucket uses.
func NewKeyedLimiter(limit int, window time.Duration, now func() time.Time) *KeyedLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &KeyedLimiter{limit: limit, window: window, now: now, entries: map[string]*fixedWindowEntry{}}
}

// Allow reports whether the (key, path) pair may proceed, counting this call
// toward its window's total either way -- matching slowapi/limits' own
// atomic increment-then-compare: a refused call is still a hit, and it does
// not start a new window early.
func (l *KeyedLimiter) Allow(key, path string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.sweep(now)
	entryKey := key + "\x00" + path
	entry := l.entries[entryKey]
	isNewPair := entry == nil
	if entry == nil || now.Sub(entry.start) >= l.window {
		if isNewPair && len(l.entries) >= maxKeyedLimiterEntries {
			// The map is saturated and this pair has never been seen: fail
			// closed rather than grow past the cap. An existing pair (a
			// window rollover, not a brand-new key) is never refused this
			// way -- only admission of a NEW entry is capped.
			return false
		}
		entry = &fixedWindowEntry{start: now}
		l.entries[entryKey] = entry
	}
	entry.count++
	return entry.count <= l.limit
}

// sweep deletes every entry whose window has fully expired, at most once
// per l.window of wall-clock time -- called with l.mu already held. This
// bounds the sweep's own O(n) cost to once per window rather than every
// call, while guaranteeing an entry outlives its window by at most one
// window's length before it is reclaimed.
func (l *KeyedLimiter) sweep(now time.Time) {
	if !l.lastSweep.IsZero() && now.Sub(l.lastSweep) < l.window {
		return
	}
	l.lastSweep = now
	for key, entry := range l.entries {
		if now.Sub(entry.start) >= l.window {
			delete(l.entries, key)
		}
	}
}

// KeyedRateLimit rejects a request once its (keyFunc(r), r.URL.Path) pair
// exceeds limiter's window budget, rendering the refusal with WriteError.
// See KeyedLimiter's own doc comment for why this is a separate wrapper
// from RateLimit/RateLimitWith (unauthenticated, per-route only) and where
// in a route's chain it belongs: around the ALREADY-authenticated inner
// handler, never at the mux level.
func KeyedRateLimit(limiter *KeyedLimiter, keyFunc KeyFunc) func(http.Handler) http.Handler {
	return KeyedRateLimitWith(limiter, keyFunc, WriteError)
}

// KeyedRateLimitWith is KeyedRateLimit with the error body rendered by
// write (see RecoverWith).
func KeyedRateLimitWith(limiter *KeyedLimiter, keyFunc KeyFunc, write ErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter != nil && !limiter.Allow(keyFunc(r), r.URL.Path) {
				write(w, r, CodeRateLimited)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
