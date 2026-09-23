package httpapi

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"
)

func TestForwardedSchemeIsUvicorns(t *testing.T) {
	for _, tc := range []struct {
		name, allow, peer string
		headers           []string
		tls               bool
		want              string
	}{
		{"default trusts loopback", "127.0.0.1", "127.0.0.1:5000", []string{"https"}, false, "https"},
		{"default ignores others", "127.0.0.1", "10.0.0.5:5000", []string{"https"}, false, "http"},
		{"tls without header", "127.0.0.1", "10.0.0.5:5000", nil, true, "https"},
		{"trusted peer downgrades tls", "*", "10.0.0.5:5000", []string{"http"}, true, "http"},
		{"last value wins", "*", "10.0.0.5:5000", []string{"https", "http"}, false, "http"},
		{"stripped", "*", "10.0.0.5:5000", []string{" \x1chttps\xa0"}, false, "https"},
		{"unknown proto ignored", "*", "10.0.0.5:5000", []string{"HTTPS"}, false, "http"},
		{"ws kept for http scope", "*", "10.0.0.5:5000", []string{"wss"}, false, "wss"},
		{"network", "10.0.0.0/8", "10.9.9.9:1", []string{"https"}, false, "https"},
		{"host-bit network is a literal", "10.0.0.1/8", "10.9.9.9:1", []string{"https"}, false, "http"},
		{"mapped v4 is not v4", "127.0.0.1", "[::ffff:127.0.0.1]:1", []string{"https"}, false, "http"},
		{"list with spaces", " 10.0.0.1 , 127.0.0.1", "127.0.0.1:1", []string{"https"}, false, "https"},
		{"empty value trusts nobody", "", "127.0.0.1:1", []string{"https"}, false, "http"},
	} {
		request := httptest.NewRequest("GET", "http://h/x/", nil)
		request.RemoteAddr = tc.peer
		for _, value := range tc.headers {
			request.Header.Add("X-Forwarded-Proto", value)
		}
		if tc.tls {
			request.TLS = &tls.ConnectionState{}
		}
		if got := ParseForwardedTrust(tc.allow).scheme(request); got != tc.want {
			t.Errorf("%s: scheme %q, want %q", tc.name, got, tc.want)
		}
	}
}
