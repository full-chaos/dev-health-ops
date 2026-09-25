package webhookintake

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// hitPagerDuty sends one delivery with no subscription header (a 401 after
// the limiter) to the binding path from peer, optionally with an
// X-Forwarded-For header.
func hitPagerDuty(handler http.HandlerFunc, binding, peer, forwarded string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+binding, nil)
	request.SetPathValue("binding_id", binding)
	request.RemoteAddr = peer + ":4242"
	if forwarded != "" {
		request.Header.Set("X-Forwarded-For", forwarded)
	}
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// The limiter is slowapi's: a fixed 60/minute window per (client IP, exact
// path), so the 61st delivery to one binding from one IP is a 429 and a
// second binding has its own budget.
func TestPagerDutyLimitIsPerIPAndBindingPath(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	now := time.Now()
	deps := Deps{Counters: httpapi.NewMemoryCounters(func() time.Time { return now })}
	deps.limiters = newRateLimiters(deps.Counters, deps.Now)
	handler := deps.handlePagerDutyWebhook()
	binding, other := uuid.NewString(), uuid.NewString()
	for i := range pagerdutyLimitPerMinute {
		if got := hitPagerDuty(handler, binding, "192.0.2.1", "").Code; got != http.StatusUnauthorized {
			t.Fatalf("delivery %d = %d, want 401 (past the limiter)", i+1, got)
		}
	}
	limited := hitPagerDuty(handler, binding, "192.0.2.1", "")
	if limited.Code != http.StatusTooManyRequests || limited.Body.String() != `{"detail":{"message":"Rate limit exceeded. Please try again later."}}` {
		t.Fatalf("61st delivery = %d %s, want the 429", limited.Code, limited.Body.String())
	}
	if got := hitPagerDuty(handler, other, "192.0.2.1", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("a second binding's first delivery = %d, want 401 (its own budget)", got)
	}
	if got := hitPagerDuty(handler, binding, "192.0.2.2", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("another IP's first delivery = %d, want 401 (its own budget)", got)
	}
}

// slowapi's get_forwarded_ip honours X-Forwarded-For only from a peer listed
// in TRUSTED_PROXIES. A header from any other peer must not mint a fresh
// budget (the previous Go key trusted every X-Forwarded-For).
func TestPagerDutyForwardedForFromAnUntrustedPeerCannotMintABudget(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	deps := Deps{}
	deps.limiters = newRateLimiters(nil, nil)
	handler := deps.handlePagerDutyWebhook()
	binding := uuid.NewString()
	for range pagerdutyLimitPerMinute {
		hitPagerDuty(handler, binding, "192.0.2.1", "")
	}
	if got := hitPagerDuty(handler, binding, "192.0.2.1", "203.0.113.9").Code; got != http.StatusTooManyRequests {
		t.Fatalf("a spoofed X-Forwarded-For from an untrusted peer = %d, want 429 (same budget)", got)
	}
	// The same header from a listed proxy IS the client's address.
	t.Setenv("TRUSTED_PROXIES", "192.0.2.1")
	if got := hitPagerDuty(handler, binding, "192.0.2.1", "203.0.113.9").Code; got != http.StatusUnauthorized {
		t.Fatalf("X-Forwarded-For from a trusted proxy = %d, want 401 (the forwarded client's own budget)", got)
	}
}

// Two api replicas over one store share one budget: 30 deliveries to each
// are 60 in the window, so the next one is a 429 whichever replica takes it.
func TestPagerDutyLimitHoldsAcrossReplicasSharingAStore(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	store := httpapi.NewMemoryCounters(nil)
	a, b := Deps{Counters: store}, Deps{Counters: store}
	a.limiters, b.limiters = newRateLimiters(store, nil), newRateLimiters(store, nil)
	handlerA, handlerB := a.handlePagerDutyWebhook(), b.handlePagerDutyWebhook()
	binding := uuid.NewString()
	for range pagerdutyLimitPerMinute / 2 {
		hitPagerDuty(handlerA, binding, "192.0.2.1", "")
		hitPagerDuty(handlerB, binding, "192.0.2.1", "")
	}
	for name, handler := range map[string]http.HandlerFunc{"replica a": handlerA, "replica b": handlerB} {
		if got := hitPagerDuty(handler, binding, "192.0.2.1", "").Code; got != http.StatusTooManyRequests {
			t.Fatalf("%s: delivery past the shared budget = %d, want 429", name, got)
		}
	}
}

type failingCounters struct{}

func (failingCounters) Increment(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("dial tcp 10.0.0.9:6379: connection refused")
}
func (failingCounters) Peek(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("dial tcp 10.0.0.9:6379: connection refused")
}
func (failingCounters) Backend() string { return "redis" }

// A store that cannot answer is the Python api's unhandled 500 (slowapi has
// no swallow_errors), marked as such, with nothing of the failure in the
// body, counted on the operator series -- never a request let through.
func TestPagerDutyStoreErrorIsTheUnhandled500(t *testing.T) {
	deps := Deps{Counters: failingCounters{}}
	deps.limiters = newRateLimiters(deps.Counters, nil)
	handler := deps.handlePagerDutyWebhook()
	recorder := hitPagerDuty(handler, uuid.NewString(), "192.0.2.1", "")
	if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` {
		t.Fatalf("store down = %d %s, want the generic 500", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get(policy.UnhandledErrorHeader) != "1" {
		t.Fatal("the 500 is not marked unhandled")
	}
	var scrape strings.Builder
	if err := httpapi.RateLimitStoreErrors.WritePrometheus(&scrape); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(scrape.String(), `dev_health_api_rate_limit_store_errors_total{limit="webhook_pagerduty"} 1`) {
		t.Fatalf("the store error is not counted on the operator series:\n%s", scrape.String())
	}
}
