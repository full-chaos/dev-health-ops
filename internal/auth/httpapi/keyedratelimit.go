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
	key   string
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

// maxKeyedLimiterEntriesPerKey bounds how many live (key, path) pairs ONE
// key may hold. A route whose path carries a caller-chosen id lets an
// authenticated caller mint unlimited distinct paths (the handler validates
// the id only AFTER this limiter runs, as Python does); with only the global
// cap above, one admin filling it made every OTHER admin's first request a
// 429 for the rest of the window (reproduced by the review round on the
// invite route with 100,000 distinct paths from one admin). Bounding each
// key means an admin that exhausts its own allowance of distinct paths is
// refused for NEW paths and nobody else is. 1,000 distinct paths per admin
// per window is far above any real use (an admin touching a handful of
// users or orgs an hour), and 100 such keys reach the global backstop, which
// stays as the bound against many-keys growth.
const maxKeyedLimiterEntriesPerKey = 1_000

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
	// keyPaths holds each key's entry keys (a key's live pairs), for
	// maxKeyedLimiterEntriesPerKey. It is a set, not a counter, so a key at
	// its bound can be re-checked against ITS OWN entries' expiry in
	// O(bound) without waiting for the periodic full sweep: a counter kept
	// only by that sweep let expired-but-unswept entries hold an admin's
	// allowance for up to a window (found by review of the first version).
	keyPaths map[string]map[string]struct{}
	// perKeyBound and globalBound are the two caps above; fields so a test
	// can exercise the same logic at a small size.
	perKeyBound, globalBound int
	// globalNextExpiry and keyNextExpiry are LOWER BOUNDS on when the
	// earliest entry (overall, and per key) can expire; the zero time means
	// "unknown, scan". A forced sweep or a key scan runs only once now has
	// reached the bound, i.e. only when something HAS expired, so it always
	// frees at least one entry and never runs pointlessly -- and, unlike a
	// time-based cooldown, it can never be skipped while an expired entry
	// is still holding a cap (a cooldown did exactly that for up to a second
	// in review of the second version). A rollover only moves an entry's
	// expiry later, so a stored bound is never later than the truth.
	globalNextExpiry time.Time
	keyNextExpiry    map[string]time.Time
	// fullSweeps and keyScans count the on-demand passes, for tests.
	fullSweeps, keyScans int
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
	return &KeyedLimiter{limit: limit, window: window, now: now, entries: map[string]*fixedWindowEntry{}, keyPaths: map[string]map[string]struct{}{}, keyNextExpiry: map[string]time.Time{},
		perKeyBound: maxKeyedLimiterEntriesPerKey, globalBound: maxKeyedLimiterEntries}
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
		if isNewPair {
			// A key that already holds its share of entries is refused for
			// NEW paths only: the cost of minting paths falls on the minter,
			// never on another caller. Expired entries of THAT key are
			// reclaimed first, so a key never stays locked out by entries
			// whose windows have already ended.
			if len(l.keyPaths[key]) >= l.perKeyBound {
				l.expireKey(key, now)
				if len(l.keyPaths[key]) >= l.perKeyBound {
					return false
				}
			}
			// The map is saturated and this pair has never been seen: fail
			// closed rather than grow past the cap -- after a full sweep of
			// expired entries (rate limited), so stale entries do not hold
			// the cap either. An existing pair (a window rollover, not a
			// brand-new key) is never refused this way -- only admission of
			// a NEW entry is capped.
			if len(l.entries) >= l.globalBound {
				l.forceSweep(now)
				if len(l.entries) >= l.globalBound {
					return false
				}
			}
			if l.keyPaths[key] == nil {
				l.keyPaths[key] = map[string]struct{}{}
			}
			l.keyPaths[key][entryKey] = struct{}{}
			if len(l.keyPaths[key]) == 1 {
				l.keyNextExpiry[key] = now.Add(l.window)
			}
			if len(l.entries) == 0 {
				l.globalNextExpiry = now.Add(l.window)
			}
		}
		entry = &fixedWindowEntry{key: key, start: now}
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
	l.sweepAll(now)
}

// sweepAll deletes every expired entry, from the entry map and from its
// key's set, and recomputes the next-expiry bounds exactly.
func (l *KeyedLimiter) sweepAll(now time.Time) {
	l.lastSweep = now
	var globalNext time.Time
	keyNext := map[string]time.Time{}
	for entryKey, entry := range l.entries {
		if now.Sub(entry.start) >= l.window {
			l.drop(entryKey, entry)
			continue
		}
		expiry := entry.start.Add(l.window)
		if globalNext.IsZero() || expiry.Before(globalNext) {
			globalNext = expiry
		}
		if current, ok := keyNext[entry.key]; !ok || expiry.Before(current) {
			keyNext[entry.key] = expiry
		}
	}
	l.globalNextExpiry, l.keyNextExpiry = globalNext, keyNext
}

// forceSweep is sweepAll for a saturated map, run only once something can
// have expired.
func (l *KeyedLimiter) forceSweep(now time.Time) {
	if !l.globalNextExpiry.IsZero() && now.Before(l.globalNextExpiry) {
		return
	}
	l.fullSweeps++
	l.sweepAll(now)
}

// expireKey deletes the expired entries of one key: O(that key's bound), run
// only once something of the key's can have expired.
func (l *KeyedLimiter) expireKey(key string, now time.Time) {
	if next, ok := l.keyNextExpiry[key]; ok && now.Before(next) {
		return
	}
	l.keyScans++
	var next time.Time
	for entryKey := range l.keyPaths[key] {
		entry := l.entries[entryKey]
		if entry == nil || now.Sub(entry.start) >= l.window {
			l.drop(entryKey, entry)
			if entry == nil {
				delete(l.keyPaths[key], entryKey)
			}
			continue
		}
		if expiry := entry.start.Add(l.window); next.IsZero() || expiry.Before(next) {
			next = expiry
		}
	}
	if next.IsZero() {
		delete(l.keyNextExpiry, key)
	} else {
		l.keyNextExpiry[key] = next
	}
}

// drop removes one entry from the map and from its key's set.
func (l *KeyedLimiter) drop(entryKey string, entry *fixedWindowEntry) {
	delete(l.entries, entryKey)
	if entry == nil {
		return
	}
	if set := l.keyPaths[entry.key]; set != nil {
		delete(set, entryKey)
		if len(set) == 0 {
			delete(l.keyPaths, entry.key)
			delete(l.keyNextExpiry, entry.key)
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
// against the limit. Validating inside the handler, behind KeyedRateLimit,
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
// use this wrapper (use KeyedRateLimitWith), and a nil validator that
// silently limited first would reintroduce exactly the divergence this
// exists to end.
func ValidateThenLimit(validate RequestValidator, limiter *KeyedLimiter, keyFunc KeyFunc, write ErrorWriter) func(http.Handler) http.Handler {
	if validate == nil {
		panic("httpapi: ValidateThenLimit needs a validator")
	}
	limit := KeyedRateLimitWith(limiter, keyFunc, write)
	return func(next http.Handler) http.Handler {
		limited := limit(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			checked, ok := validate(w, r)
			if !ok {
				return
			}
			limited.ServeHTTP(w, checked)
		})
	}
}
