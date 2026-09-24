package httpapi

import (
	"context"
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

// HitStore counts hits for a Limit. It is the ONE seam every limited route
// goes through, so the counters can live in one shared store (Valkey, the
// backend the Python api uses through REDIS_URL) and the limit holds across
// api replicas, or in process memory for development.
//
// Hit records one hit for (limit, key, path) and reports whether it is
// within the limit. A refused hit still counts (slowapi's atomic
// increment-then-compare). An error means the store could not say: the
// middleware answers the Python api's unhandled-error 500 and never lets
// the request through.
type HitStore interface {
	Hit(ctx context.Context, limit Limit, key, path string) (allowed bool, err error)
	// Backend names the store as /health reports it: "redis" for the shared
	// store (Python's own word for it), "memory" for the in-process one.
	Backend() string
}

// MemoryStore is the in-process HitStore: one KeyedLimiter per limit ID. It
// is per process, so with several api replicas the effective limit is per
// replica -- acceptable for development and tests, which is why the api
// refuses to start outside development without the shared store.
type MemoryStore struct {
	now func() time.Time

	mu       sync.Mutex
	limiters map[string]*KeyedLimiter
}

// NewMemoryStore returns an in-process store; now is injectable for tests
// (nil means time.Now).
func NewMemoryStore(now func() time.Time) *MemoryStore {
	return &MemoryStore{now: now, limiters: map[string]*KeyedLimiter{}}
}

// Hit implements HitStore.
func (m *MemoryStore) Hit(_ context.Context, limit Limit, key, path string) (bool, error) {
	m.mu.Lock()
	limiter, ok := m.limiters[limit.ID]
	if !ok {
		limiter = NewKeyedLimiter(limit.Count, limit.Window, m.now)
		m.limiters[limit.ID] = limiter
	}
	m.mu.Unlock()
	// A nil limiter (non-positive limit or window) allows everything: the
	// same degenerate-configuration contract NewKeyedLimiter documents.
	return limiter.Allow(key, path), nil
}

// Backend implements HitStore.
func (m *MemoryStore) Backend() string { return "memory" }

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
// exceeds limit in store, rendering the refusal with write. A store error is
// the Python api's unhandled-error 500 (slowapi has no swallow_errors), never
// a silent pass: it is logged with the limit's ID (never the caller key or
// path) and counted in RateLimitStoreErrors.
func LimitWith(store HitStore, limit Limit, keyFunc KeyFunc, write ErrorWriter) func(http.Handler) http.Handler {
	RateLimitStoreErrors.declare(limit.ID)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			allowed, err := store.Hit(r.Context(), limit, keyFunc(r), r.URL.Path)
			if err != nil {
				slog.ErrorContext(r.Context(), "rate limit store failed; answering 500",
					slog.String("limit", limit.ID), slog.String("backend", store.Backend()), slog.String("error", err.Error()))
				RateLimitStoreErrors.inc(limit.ID)
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
