package audit

import (
	"net/http/httptest"
	"testing"
)

// CHAOS-7204: audit ip_address uses the same client-IP rule as the limiter.
func TestRequestMetadataIPIgnoresSpoofedForwardingHeaders(t *testing.T) {
	cases := []struct {
		name, trusted, peer, xff, want string
	}{
		{"untrusted peer, spoofed xff -> peer", "10.0.0.0/8", "203.0.113.5:1", "1.2.3.4", `{"ip_address": "203.0.113.5"}`},
		{"trusted peer, spoofed leftmost -> rightmost untrusted", "10.0.0.0/8", "10.0.0.5:1", "1.2.3.4, 198.51.100.7, 10.0.0.9", `{"ip_address": "198.51.100.7"}`},
		{"trusted list unset -> peer", "", "10.0.0.5:1", "1.2.3.4", `{"ip_address": "10.0.0.5"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRUSTED_PROXIES", tc.trusted)
			request := httptest.NewRequest("POST", "/x", nil)
			request.RemoteAddr = tc.peer
			request.Header.Set("X-Forwarded-For", tc.xff)
			got, err := RequestMetadata(request)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
