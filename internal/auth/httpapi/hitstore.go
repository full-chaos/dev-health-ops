package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Limit is one route's rate limit: at most Count hits per Window per
// (caller key, exact request path), the fixed-window rule of slowapi's
// default strategy. ID names the limit in the shared store and in logs and
// metrics; it must be stable and unique per limit ("admin_password").
type Limit struct {
	ID     string
	Count  int
	Window time.Duration
}

// Hit is one request's claim on a counter: the limit it counts against, the
// caller key and the exact request path. The store scopes the counter to
// (Limit.ID, Key, Path) and runs the fixed window of Limit.Window.
type Hit struct {
	Limit Limit
	Key   string
	Path  string
}

// ErrPathBound is what a CounterStore returns when it cannot admit a
// BRAND-NEW (key, path) counter: the request is refused and nothing is
// counted. Only MemoryCounters returns it, at its global entry cap
// (maxKeyedLimiterEntries), a memory-safety backstop for the in-process store
// that no deployment counts in. The shared store never returns it: nothing
// bounds how many distinct paths a caller may count there, exactly as
// slowapi bounds nothing (a per-caller bound refused a client Python serves,
// CHAOS-6624).
var ErrPathBound = errors.New("httpapi: in-process rate-limit counter set is full")

// CounterStore is the ONE store every rate limiter counts in. The shared
// implementation (ratelimitvalkey.Store, Valkey -- the backend the Python api
// reaches through REDIS_URL) holds a limit across api replicas; MemoryCounters
// is the in-process one for development and tests.
//
// Increment adds one hit and returns the window's count including it. A
// refused hit still counts (slowapi's atomic increment-then-compare). Any
// error other than ErrPathBound means the store could not say: the limiter
// reports it and the middleware answers the Python api's unhandled-error 500,
// never letting the request through.
type CounterStore interface {
	Increment(ctx context.Context, hit Hit) (count int64, err error)
	// Backend names the store as /health reports it: "redis" for the shared
	// store (Python's own word for it), "memory" for the in-process one.
	Backend() string
}

// MemoryCounters is the in-process CounterStore: one set of window counters
// per limit ID. It is per process, so with several api replicas the effective
// limit is per replica -- acceptable for development and tests, which is why
// the api refuses to start outside development without the shared store.
type MemoryCounters struct {
	now func() time.Time

	mu      sync.Mutex
	windows map[string]*windowCounters
}

var _ CounterStore = (*MemoryCounters)(nil)

// NewMemoryCounters returns an in-process store; now is injectable for tests
// (nil means time.Now).
func NewMemoryCounters(now func() time.Time) *MemoryCounters {
	return &MemoryCounters{now: now, windows: map[string]*windowCounters{}}
}

// Increment implements CounterStore.
func (m *MemoryCounters) Increment(_ context.Context, hit Hit) (int64, error) {
	m.mu.Lock()
	counters, ok := m.windows[hit.Limit.ID]
	if !ok {
		counters = newWindowCounters(hit.Limit.Window, m.now)
		if counters == nil {
			m.mu.Unlock()
			return 0, errors.New("httpapi: limit has no window")
		}
		m.windows[hit.Limit.ID] = counters
	}
	m.mu.Unlock()
	count, admitted := counters.hit(hit.Key, hit.Path)
	if !admitted {
		return 0, ErrPathBound
	}
	return int64(count), nil
}

// Backend implements CounterStore.
func (m *MemoryCounters) Backend() string { return "memory" }

// KeyedLimiter is the one rate limiter: a fixed window of Limit.Count hits
// per Limit.Window per (caller key, exact request path), counted in a
// CounterStore. It is the Go equivalent of Python's slowapi default strategy
// (the `limits` package's FixedWindowRateLimiter), confirmed against live
// slowapi: a window starts on the caller's first hit after the previous one
// expired and lasts exactly the window -- not epoch-aligned, and not reset by
// a refused hit. Two (key, path) pairs never share a bucket.
//
// It is applied INSIDE a route's handler chain (LimitWith,
// ValidateThenLimit), after authentication has resolved the key.
type KeyedLimiter struct {
	store CounterStore
	limit Limit
}

// NewKeyedLimiter returns a limiter over store. A limit with no ID, a
// non-positive count or a non-positive window yields nil, which allows
// everything (the degenerate-configuration contract NewBucket uses).
func NewKeyedLimiter(store CounterStore, limit Limit) *KeyedLimiter {
	if store == nil || limit.ID == "" || limit.Count <= 0 || limit.Window <= 0 {
		return nil
	}
	return &KeyedLimiter{store: store, limit: limit}
}

// ID is the limit id, "" for a nil limiter.
func (l *KeyedLimiter) ID() string {
	if l == nil {
		return ""
	}
	return l.limit.ID
}

// Backend names the store the limiter counts in.
func (l *KeyedLimiter) Backend() string {
	if l == nil {
		return "noop"
	}
	return l.store.Backend()
}

// Allow counts one hit for (key, path) and reports whether it is within the
// limit. A caller at its path bound is refused with a nil error. A store
// error is returned and the hit must be treated as refused.
func (l *KeyedLimiter) Allow(ctx context.Context, key, path string) (bool, error) {
	if l == nil {
		return true, nil
	}
	count, err := l.store.Increment(ctx, Hit{Limit: l.limit, Key: key, Path: path})
	if errors.Is(err, ErrPathBound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return count <= int64(l.limit.Count), nil
}

// StoreErrors counts rate-limit store errors per limit id and writes them in
// the operator endpoint's Prometheus format (it is a health.MetricsSource).
// The api process installs no OTel meter provider, so an OTel instrument
// would count into nothing; this is registered on the operator /metrics
// instead, where a scrape can alert on it.
type StoreErrors struct {
	mu     sync.Mutex
	counts map[string]uint64
}

// RateLimitStoreErrors is the process-wide counter LimitWith increments; the
// api registers it as a metrics source at startup.
var RateLimitStoreErrors = &StoreErrors{counts: map[string]uint64{}}

// declare makes a limit's series exist at zero, so an alert can be written
// against the absence of failure and not only the absence of a series.
func (c *StoreErrors) declare(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.counts[id]; !ok {
		c.counts[id] = 0
	}
}

func (c *StoreErrors) inc(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[id]++
}

// WritePrometheus implements the operator registry's MetricsSource. Label
// values are limit ids (fixed, code-defined), never a caller key or a path.
func (c *StoreErrors) WritePrometheus(w io.Writer) error {
	c.mu.Lock()
	ids := make([]string, 0, len(c.counts))
	for id := range c.counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := make([]string, len(ids))
	for i, id := range ids {
		lines[i] = fmt.Sprintf("dev_health_api_rate_limit_store_errors_total{limit=%s} %d\n", strconv.Quote(id), c.counts[id])
	}
	c.mu.Unlock()
	if _, err := io.WriteString(w, "# HELP dev_health_api_rate_limit_store_errors_total Rate-limit store errors on limited routes; each answered a 500.\n"+
		"# TYPE dev_health_api_rate_limit_store_errors_total counter\n"); err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
	}
	return nil
}

// LimitWith rejects a request once its (keyFunc(r), r.URL.Path) pair
// exceeds its limiter, rendering the refusal with write. A store error is
// the Python api's unhandled-error 500 (slowapi has no swallow_errors), never
// a silent pass: it is logged with the limit's ID (never the caller key or
// path) and counted in RateLimitStoreErrors.
func LimitWith(limiter *KeyedLimiter, keyFunc KeyFunc, write ErrorWriter) func(http.Handler) http.Handler {
	id := limiter.ID()
	if limiter != nil {
		RateLimitStoreErrors.declare(id)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			allowed, err := limiter.Allow(r.Context(), keyFunc(r), r.URL.Path)
			if err != nil {
				slog.ErrorContext(r.Context(), "rate limit store failed; answering 500",
					slog.String("limit", id), slog.String("backend", limiter.Backend()), slog.String("error", err.Error()))
				RateLimitStoreErrors.inc(id)
				write(w, r, CodeInternal)
				return
			}
			if !allowed {
				write(w, r, CodeRateLimited)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
