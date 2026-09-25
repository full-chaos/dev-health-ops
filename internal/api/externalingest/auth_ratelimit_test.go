package externalingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// authFrom sends one request with no bearer (an auth failure that never
// reaches the database) from peer through requireIngestScope.
func authFrom(deps Deps, peer string) *ingestError {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/batches", nil)
	request.RemoteAddr = peer + ":4242"
	_, failure := deps.requireIngestScope(context.Background(), request, "ingest:status", false)
	return failure
}

// auth.py's failure throttle is slowapi's `limits` limiter driven directly: a
// FIXED 30/minute window per address, test() before the request and hit() on
// each failure, in the storage every replica shares. The 31st failure from
// one address is refused BEFORE it is judged, with the "failed" message; a
// second address has its own budget; and nothing refills inside the window.
func TestAuthFailureThrottleIsAFixedWindowPerAddress(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	now := time.Now()
	deps := Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })}
	deps.limiters = newAuthLimiters(deps.Counters, deps.Now)
	for i := range ingestAuthFailureLimitPerMinute {
		if failure := authFrom(deps, "192.0.2.1"); failure == nil || failure.Status != http.StatusUnauthorized {
			t.Fatalf("failure %d = %+v, want the 401", i+1, failure)
		}
	}
	failure := authFrom(deps, "192.0.2.1")
	if failure == nil || failure.Status != http.StatusTooManyRequests || failure.Message != "Too many failed authentication attempts" {
		t.Fatalf("31st failure = %+v, want the failed-attempts 429", failure)
	}
	// A token bucket would have refilled by now; a fixed window has not.
	now = now.Add(30 * time.Second)
	if failure := authFrom(deps, "192.0.2.1"); failure == nil || failure.Status != http.StatusTooManyRequests {
		t.Fatalf("31st failure 30s later = %+v, want still the 429 (nothing refills inside a fixed window)", failure)
	}
	if failure := authFrom(deps, "192.0.2.2"); failure == nil || failure.Status != http.StatusUnauthorized {
		t.Fatalf("another address = %+v, want its own budget (401)", failure)
	}
	now = now.Add(31 * time.Second)
	if failure := authFrom(deps, "192.0.2.1"); failure == nil || failure.Status != http.StatusUnauthorized {
		t.Fatalf("after the window = %+v, want a new window (401)", failure)
	}
}

// The attempt ceiling counts every attempt (100/minute per address) and is
// checked first.
func TestAuthAttemptCeilingCountsEveryAttempt(t *testing.T) {
	deps := Deps{}
	deps.limiters = newAuthLimiters(nil, nil)
	for i := range ingestAuthAttemptLimitPerMinute {
		if allowed, err := deps.limiters.attempt.Allow(context.Background(), "192.0.2.1", ""); err != nil || !allowed {
			t.Fatalf("attempt %d = %v, %v, want allowed", i+1, allowed, err)
		}
	}
	failure := authFrom(deps, "192.0.2.1")
	if failure == nil || failure.Status != http.StatusTooManyRequests || failure.Message != "Too many authentication attempts" {
		t.Fatalf("101st attempt = %+v, want the attempts 429", failure)
	}
}

// Two api replicas over one store share the budget: 15 failures at each are
// 30 in the window, so the next is refused whichever replica takes it.
func TestAuthFailureThrottleHoldsAcrossReplicasSharingAStore(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	store := httpapi.NewMemoryCounters(nil)
	a, b := Deps{Counters: store}, Deps{Counters: store}
	a.limiters, b.limiters = newAuthLimiters(store, nil), newAuthLimiters(store, nil)
	for range ingestAuthFailureLimitPerMinute / 2 {
		authFrom(a, "192.0.2.1")
		authFrom(b, "192.0.2.1")
	}
	for name, deps := range map[string]Deps{"replica a": a, "replica b": b} {
		if failure := authFrom(deps, "192.0.2.1"); failure == nil || failure.Status != http.StatusTooManyRequests {
			t.Fatalf("%s past the shared budget = %+v, want the 429", name, failure)
		}
	}
}

// A store that cannot answer is the Python api's unhandled 500 on every
// limiter read and write (the `limits` storage raises, auth.py does not catch
// it), never a request let through.
func TestAuthLimiterStoreErrorIsTheUnhandled500(t *testing.T) {
	deps := Deps{}
	deps.limiters = newAuthLimiters(failingCounters{}, nil)
	failure := authFrom(deps, "192.0.2.1")
	if failure == nil || failure.Status != http.StatusInternalServerError || !failure.Unhandled {
		t.Fatalf("store down = %+v, want the unhandled 500", failure)
	}
}
