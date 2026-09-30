package ratelimit

import (
	"net/http/httptest"
	"testing"
)

func TestForwardedIPTrustsTheHeaderOnlyFromATrustedPeer(t *testing.T) {
	request := httptest.NewRequest("POST", "/api/v1/auth/login", nil)
	request.RemoteAddr = "10.0.0.5:4321"
	request.Header.Set("X-Forwarded-For", " 203.0.113.7 , 10.0.0.9")
	t.Setenv("TRUSTED_PROXIES", "")
	if got := ForwardedIP(request); got != "10.0.0.5" {
		t.Fatalf("untrusted peer: got %q", got)
	}
	t.Setenv("TRUSTED_PROXIES", "10.0.0.9, 10.0.0.5")
	if got := ForwardedIP(request); got != "203.0.113.7" {
		t.Fatalf("trusted peer: got %q", got)
	}
	request.Header.Del("X-Forwarded-For")
	if got := ForwardedIP(request); got != "10.0.0.5" {
		t.Fatalf("trusted peer without the header: got %q", got)
	}
	request.RemoteAddr = ""
	if got := ForwardedIP(request); got != "unknown" {
		t.Fatalf("no peer: got %q", got)
	}
}

// CHAOS-7204: the limiter key is the rightmost untrusted hop. A client that
// writes a different leftmost X-Forwarded-For entry on every request must keep
// landing in ONE bucket, and an untrusted peer's header must not pick a bucket.
func TestForwardedIPIgnoresASpoofedLeftmostHop(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "10.0.0.0/8")
	keys := map[string]bool{}
	for _, spoof := range []string{"1.1.1.1", "2.2.2.2", "not-an-ip", "3.3.3.3, 4.4.4.4"} {
		request := httptest.NewRequest("POST", "/api/v1/auth/login", nil)
		request.RemoteAddr = "10.0.0.5:4321"
		request.Header.Set("X-Forwarded-For", spoof+", 198.51.100.7, 10.0.0.9")
		keys[ForwardedIP(request)] = true
	}
	if len(keys) != 1 || !keys["198.51.100.7"] {
		t.Fatalf("spoofed leftmost hops changed the key: %v", keys)
	}
	direct := httptest.NewRequest("POST", "/api/v1/auth/login", nil)
	direct.RemoteAddr = "203.0.113.5:4321"
	direct.Header.Set("X-Forwarded-For", "9.9.9.9")
	if got := ForwardedIP(direct); got != "203.0.113.5" {
		t.Fatalf("untrusted peer chose its own bucket: %q", got)
	}
}
