package externalingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func TestKeyedBucketAllowsUpToBurstThenBlocks(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	bucket := newKeyedBucket(60, 2, clock) // 1/s refill, burst 2

	if !bucket.allow("a") || !bucket.allow("a") {
		t.Fatal("burst of 2 must be allowed")
	}
	if bucket.allow("a") {
		t.Fatal("3rd immediate call must be blocked")
	}
	// A different key has its own budget.
	if !bucket.allow("b") {
		t.Fatal("a different key must not share a's exhausted bucket")
	}

	now = now.Add(time.Second)
	if !bucket.allow("a") {
		t.Fatal("one token must have refilled after 1s at 1/s")
	}
}

func TestKeyedBucketTestDoesNotConsume(t *testing.T) {
	now := time.Unix(0, 0)
	bucket := newKeyedBucket(60, 1, func() time.Time { return now })
	for i := 0; i < 5; i++ {
		if !bucket.test("k") {
			t.Fatalf("test() must never consume a token (call %d)", i)
		}
	}
	if !bucket.allow("k") {
		t.Fatal("the untouched token must still be there")
	}
}

func TestRouteLimitersEnforceThePerMinuteCeilings(t *testing.T) {
	now := time.Unix(0, 0)
	limiters := newRouteLimiters(nil, func() time.Time { return now })

	cases := []struct {
		name   string
		bucket *httpapi.KeyedLimiter
		ceil   int
	}{
		{"schemasList", limiters.schemasList, ingestReadLimitPerMinute},
		{"schemasGet", limiters.schemasGet, ingestReadLimitPerMinute},
		{"listBatches", limiters.listBatches, ingestReadLimitPerMinute},
		{"getBatch", limiters.getBatch, ingestReadLimitPerMinute},
		{"validate", limiters.validate, ingestValidateLimitPerMinute},
		{"batches", limiters.batches, ingestBatchLimitPerMinute},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := "k"
			for i := 0; i < c.ceil; i++ {
				if err := rateLimitedOrTooManyRequests(ctx, c.bucket, key, "/p"); err != nil {
					t.Fatalf("request %d/%d unexpectedly rate-limited: %v", i+1, c.ceil, err)
				}
			}
			err := rateLimitedOrTooManyRequests(ctx, c.bucket, key, "/p")
			if err == nil || err.Status != 429 || err.Code != "rate_limited" {
				t.Fatalf("request %d must be rate-limited, got %v", c.ceil+1, err)
			}
			// A different key has its own budget -- per-caller keying, not
			// one bucket shared by every caller of a route.
			if err := rateLimitedOrTooManyRequests(ctx, c.bucket, "different-key", "/p"); err != nil {
				t.Fatalf("a different key must not share an exhausted bucket: %v", err)
			}
		})
	}
}

func TestRateLimitedOrTooManyRequestsWithANilBucketAlwaysAllows(t *testing.T) {
	if err := rateLimitedOrTooManyRequests(ctx, nil, "k", "/p"); err != nil {
		t.Fatalf("a nil bucket (unrate-limited route, e.g. availability) must never refuse: %v", err)
	}
}

func TestIngestTokenRateLimitKeyIsStablePerToken(t *testing.T) {
	a := ingestTokenRateLimitKey("token-1")
	b := ingestTokenRateLimitKey("token-1")
	c := ingestTokenRateLimitKey("token-2")
	if a != b {
		t.Fatal("the same token id must produce the same key")
	}
	if a == c {
		t.Fatal("different token ids must produce different keys")
	}
}

func TestForwardedIPUsesPeerUnlessTrusted(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "10.0.0.1")

	untrusted := httptest.NewRequest(http.MethodGet, "/", nil)
	untrusted.RemoteAddr = "203.0.113.5:1234"
	untrusted.Header.Set("X-Forwarded-For", "198.51.100.9")
	if got := forwardedIP(untrusted); got != "203.0.113.5" {
		t.Errorf("untrusted peer: got %q, want the TCP peer", got)
	}

	trusted := httptest.NewRequest(http.MethodGet, "/", nil)
	trusted.RemoteAddr = "10.0.0.1:1234"
	trusted.Header.Set("X-Forwarded-For", "198.51.100.9, 10.0.0.1")
	if got := forwardedIP(trusted); got != "198.51.100.9" {
		t.Errorf("trusted peer: got %q, want the forwarded header's first hop", got)
	}
}

// slowapi buckets by (key, exact path): requests for different URLs never
// share a budget (CHAOS-6480: 125 lookups of 125 distinct batch ids were
// 125x404 in Python and 120x404 + 5x429 when Go kept one bucket per token).
func TestRouteLimitersBucketPerPath(t *testing.T) {
	now := time.Unix(0, 0)
	limiters := newRouteLimiters(nil, func() time.Time { return now })
	for i := 0; i < 3*ingestReadLimitPerMinute; i++ {
		path := "/api/v1/external-ingest/batches/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", path); err != nil {
			t.Fatalf("request %d on its own path was refused: %v", i, err)
		}
	}
	for i := 0; i < ingestReadLimitPerMinute; i++ {
		if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/same"); err != nil {
			t.Fatalf("request %d on a fresh path was refused: %v", i, err)
		}
	}
	if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/same"); err == nil {
		t.Fatal("the 121st request on one path must be refused")
	}
}

// slowapi's default strategy is a fixed window that starts at the first hit
// and is not refilled meanwhile (the token bucket refilled during a burst:
// CI answered 124x404 then 429 for a 125-request burst).
func TestRouteLimitersFixedWindowDoesNotRefillWithinTheWindow(t *testing.T) {
	now := time.Unix(0, 0)
	limiters := newRouteLimiters(nil, func() time.Time { return now })
	for i := 0; i < ingestReadLimitPerMinute; i++ {
		if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/p"); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(30 * time.Second)
	if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/p"); err == nil {
		t.Fatal("half a window later the budget must still be spent")
	}
	now = now.Add(31 * time.Second)
	if err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/p"); err != nil {
		t.Fatalf("a full window after the first hit the budget is new: %v", err)
	}
}

var ctx = context.Background()

// A store that cannot answer is the Python api's unhandled 500, never a
// request let through, and the limiter counts it (a scrape can alert on it).
type failingCounters struct{ httpapi.CounterStore }

func (failingCounters) Increment(context.Context, httpapi.Hit) (int64, error) {
	return 0, errors.New("valkey down")
}
func (failingCounters) Backend() string { return "redis" }

func TestRouteLimitersAStoreErrorIsTheUnhandled500(t *testing.T) {
	limiters := newRouteLimiters(failingCounters{}, nil)
	err := rateLimitedOrTooManyRequests(ctx, limiters.getBatch, "token", "/p")
	if err == nil || err.Status != http.StatusInternalServerError {
		t.Fatalf("a store error must answer the unhandled 500, got %v", err)
	}
	var out strings.Builder
	if writeErr := httpapi.RateLimitStoreErrors.WritePrometheus(&out); writeErr != nil {
		t.Fatal(writeErr)
	}
	if !strings.Contains(out.String(), `dev_health_api_rate_limit_store_errors_total{limit="external_ingest_batches_get"} 1`) {
		t.Fatalf("the store error must be counted against its limit id:\n%s", out.String())
	}
}

// Through the route: a limit store that cannot answer is the Python api's
// unhandled 500 (the marker header the api's error middleware keys on and the
// generic body), not a 429 and not a request let through.
func TestAStoreErrorThroughTheRouteIsTheUnhandled500(t *testing.T) {
	deps := Deps{routeLimiters: newRouteLimiters(failingCounters{}, nil)}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/schemas", nil)
	recorder := httptest.NewRecorder()
	deps.handleListSchemas()(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("X-Dho-Unhandled-Error"); got != "1" {
		t.Fatalf("X-Dho-Unhandled-Error = %q, want \"1\"", got)
	}
	const want = `{"error":{"code":"internal_error","message":"Internal Server Error"}}`
	if got := strings.TrimSpace(recorder.Body.String()); got != want {
		t.Fatalf("body = %s, want %s", got, want)
	}
}
