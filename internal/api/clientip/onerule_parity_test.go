package clientip_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// C7: go-api (clientip.FromRequest over TRUSTED_PROXIES) and auth-service (clientaddr.Trust.Resolve over
// AUTH_SERVICE_TRUSTED_PROXIES) answer the same for the same request. One rule, two callers.
func TestGoAPIAndAuthServiceResolveTheSameClient(t *testing.T) {
	rows := []struct {
		name    string
		trusted string
		peer    string
		xff     []string
		realIP  string
	}{
		{"untrusted peer", "10.0.0.0/8", "203.0.113.5:4000", []string{"1.2.3.4"}, "5.6.7.8"},
		{"empty list", "", "10.0.0.5:4000", []string{"1.2.3.4"}, "5.6.7.8"},
		{"one value", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7"}, ""},
		{"chain with trusted tail", "10.0.0.0/8", "10.0.0.5:4000", []string{"9.9.9.9, 198.51.100.7, 10.0.0.9"}, ""},
		{"two lines", "10.0.0.5", "10.0.0.5:4000", []string{"6.6.6.6", "198.51.100.7"}, ""},
		{"malformed right-most", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7, garbage"}, ""},
		{"empty hop", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7,,10.0.0.5"}, ""},
		{"port", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7:5555"}, ""},
		{"bracketed v6 with port", "10.0.0.5", "10.0.0.5:4000", []string{"[2001:db8::1]:443"}, ""},
		{"zone", "10.0.0.5", "10.0.0.5:4000", []string{"fe80::1%eth0"}, ""},
		{"mapped", "10.0.0.5", "10.0.0.5:4000", []string{"::ffff:198.51.100.7"}, ""},
		{"x-real-ip, no chain", "10.0.0.5", "10.0.0.5:4000", nil, "198.51.100.7"},
		{"x-real-ip, trusted chain", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7"}, "198.51.100.7"},
		{"x-real-ip, malformed chain", "10.0.0.5", "10.0.0.5:4000", []string{"garbage"}, "5.6.7.8"},
		{"all hops trusted", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7, 10.0.0.9"}, ""},
		{"excluded address is not a proxy", "10.0.0.0/24,!10.0.0.1", "10.0.0.5:4000", []string{"198.51.100.7, 10.0.0.1"}, ""},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = row.peer
			for _, line := range row.xff {
				r.Header.Add("X-Forwarded-For", line)
			}
			if row.realIP != "" {
				r.Header.Set("X-Real-IP", row.realIP)
			}
			t.Setenv(clientip.Env, row.trusted)
			trust, err := clientaddr.ParseTrust(row.trusted)
			if err != nil {
				t.Fatal(err)
			}
			if goAPI, authService := clientip.FromRequest(r), trust.Resolve(r).String(); goAPI != authService {
				t.Fatalf("go-api = %q, auth-service = %q", goAPI, authService)
			}
		})
	}
}
