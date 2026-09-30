package admin

import (
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
)

// CHAOS-7204: requestAuditEntry records the same client IP as the limiter and
// audit.RequestMetadata; a spoofed leftmost X-Forwarded-For never lands in the row.
func TestRequestAuditEntryIgnoresSpoofedForwardingHeaders(t *testing.T) {
	cases := []struct {
		name, trusted, peer, xff, want string
	}{
		{"untrusted peer, spoofed xff -> peer", "10.0.0.0/8", "203.0.113.5:1", "1.2.3.4", `{"ip_address":"203.0.113.5"}`},
		{"trusted peer, spoofed leftmost -> rightmost untrusted", "10.0.0.0/8", "10.0.0.5:1", "1.2.3.4, 198.51.100.7, 10.0.0.9", `{"ip_address":"198.51.100.7"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRUSTED_PROXIES", tc.trusted)
			request := httptest.NewRequest("POST", "/x", nil)
			request.RemoteAddr = tc.peer
			request.Header.Set("X-Forwarded-For", tc.xff)
			got := requestAuditEntry(request, audit.Entry{}).RequestMetadata
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
