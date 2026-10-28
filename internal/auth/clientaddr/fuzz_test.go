package clientaddr

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

// FuzzParseTrust: a parsed trust list never trusts every peer, never trusts an
// excluded address, and the address it resolves is the peer unless the peer is
// trusted and sent one value.
func FuzzParseTrust(f *testing.F) {
	for _, seed := range [][3]string{
		{"10.0.0.5", "10.0.0.5:1", "198.51.100.7"},
		{"*", "10.0.0.5:1", "198.51.100.7"},
		{"10.0.0.0/8,::1", "[::1]:1", "2001:db8::1"},
		{"10.0.0.1/8", "10.0.0.5:1", "198.51.100.7, 10.0.0.9"},
		{"", "203.0.113.9:1", "unknown"},
		{"10.42.0.0/24,!10.42.0.0,!10.42.0.1", "10.42.0.1:1", "198.51.100.7"},
		{"10.42.0.0/24,!10.42.0.1", "[::ffff:10.42.0.1]:1", "198.51.100.7"},
		{"fd00::/120,!fd00::1", "[fd00::1]:1", "2001:db8::1"},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, value, remote, forwarded string) {
		trust, err := ParseTrust(value)
		if err != nil {
			return
		}
		if strings.Contains(value, "*") {
			t.Fatalf("ParseTrust(%q) accepted a wildcard", value)
		}
		for _, raw := range strings.Split(value, ",") {
			entry := strings.TrimSpace(raw)
			if !strings.HasPrefix(entry, "!") {
				continue
			}
			excluded, err := netip.ParseAddr(entry[1:])
			if err != nil {
				t.Fatalf("ParseTrust(%q) accepted exclusion %q that is not an address", value, entry)
			}
			probe := httptest.NewRequest(http.MethodGet, "/", nil)
			probe.RemoteAddr = net.JoinHostPort(excluded.String(), "1")
			if trust.TrustsPeer(probe) {
				t.Fatalf("ParseTrust(%q) trusts the excluded %v", value, excluded)
			}
		}
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = remote
		r.Header.Set("X-Forwarded-For", forwarded)
		got := trust.Resolve(r)
		host, _, splitErr := net.SplitHostPort(remote)
		if splitErr != nil {
			host = remote
		}
		peer, peerErr := netip.ParseAddr(host)
		if peerErr != nil {
			return
		}
		peer = peer.WithZone("").Unmap()
		if !trust.trusts(peer) && got != peer {
			t.Fatalf("an untrusted peer %v resolved to %v", peer, got)
		}
	})
}
