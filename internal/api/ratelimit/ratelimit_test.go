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
