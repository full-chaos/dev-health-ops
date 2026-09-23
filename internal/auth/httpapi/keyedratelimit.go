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
	// entries is unbounded in principle, but its real cardinality is
	// (distinct callers) x (distinct rate-limited paths this limiter is
	// applied to) -- for an admin-only limiter that is a small, stable
	// number (the platform's admin population x its handful of
	// rate-limited admin routes), never proportional to request volume.
	entries map[string]*fixedWindowEntry
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
	entryKey := key + "\x00" + path
	entry := l.entries[entryKey]
	if entry == nil || now.Sub(entry.start) >= l.window {
		entry = &fixedWindowEntry{start: now}
		l.entries[entryKey] = entry
	}
	entry.count++
	return entry.count <= l.limit
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
