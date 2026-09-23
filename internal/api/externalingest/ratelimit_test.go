package externalingest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
	limiters := newRouteLimiters(func() time.Time { return now })

	cases := []struct {
		name   string
		bucket *keyedBucket
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
				if err := rateLimitedOrTooManyRequests(c.bucket, key); err != nil {
					t.Fatalf("request %d/%d unexpectedly rate-limited: %v", i+1, c.ceil, err)
				}
			}
			err := rateLimitedOrTooManyRequests(c.bucket, key)
			if err == nil || err.Status != 429 || err.Code != "rate_limited" {
				t.Fatalf("request %d must be rate-limited, got %v", c.ceil+1, err)
			}
			// A different key has its own budget -- per-caller keying, not
			// one bucket shared by every caller of a route.
			if err := rateLimitedOrTooManyRequests(c.bucket, "different-key"); err != nil {
				t.Fatalf("a different key must not share an exhausted bucket: %v", err)
			}
		})
	}
}

func TestRateLimitedOrTooManyRequestsWithANilBucketAlwaysAllows(t *testing.T) {
	if err := rateLimitedOrTooManyRequests(nil, "k"); err != nil {
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
