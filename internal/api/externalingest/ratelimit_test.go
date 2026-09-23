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
