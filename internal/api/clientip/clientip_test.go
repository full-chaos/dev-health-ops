package clientip

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func request(peer string, xff []string, realIP string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = peer
	for _, line := range xff {
		r.Header.Add("X-Forwarded-For", line)
	}
	if realIP != "" {
		r.Header.Set("X-Real-IP", realIP)
	}
	return r
}

func TestFromRequest(t *testing.T) {
	cases := []struct {
		name    string
		trusted string
		peer    string
		xff     []string
		realIP  string
		want    string
	}{
		{"untrusted peer, spoofed xff -> peer", "10.0.0.0/8", "203.0.113.5:4000", []string{"1.2.3.4"}, "", "203.0.113.5"},
		{"untrusted peer, spoofed xff and x-real-ip -> peer", "10.0.0.0/8", "203.0.113.5:4000", []string{"1.2.3.4"}, "5.6.7.8", "203.0.113.5"},
		{"trusted list unset -> peer, xff ignored", "", "10.0.0.5:4000", []string{"1.2.3.4"}, "5.6.7.8", "10.0.0.5"},
		{"trusted peer, spoof+real+trusted -> real", "10.0.0.0/8", "10.0.0.5:4000", []string{"9.9.9.9, 198.51.100.7, 10.0.0.9"}, "", "198.51.100.7"},
		{"trusted peer, single real hop", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7"}, "", "198.51.100.7"},
		{"spoofed leftmost cannot change result", "10.0.0.5", "10.0.0.5:4000", []string{"6.6.6.6, 198.51.100.7"}, "", "198.51.100.7"},
		{"malformed hop LEFT of the valid untrusted hop is never read", "10.0.0.5", "10.0.0.5:4000", []string{"not-an-ip, 198.51.100.7"}, "", "198.51.100.7"},
		{"malformed rightmost hop -> peer, never wins", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7, garbage"}, "", "10.0.0.5"},
		{"malformed hop between trusted hops -> peer", "10.0.0.0/8", "10.0.0.5:4000", []string{"198.51.100.7, garbage, 10.0.0.9"}, "", "10.0.0.5"},
		{"empty hop is malformed", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7,,10.0.0.5"}, "", "10.0.0.5"},
		{"all hops trusted, no x-real-ip -> peer", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7, 10.0.0.9"}, "", "10.0.0.5"},
		{"all hops trusted, x-real-ip valid -> x-real-ip", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7"}, "198.51.100.7", "198.51.100.7"},
		{"trusted peer, no xff, x-real-ip valid", "10.0.0.5", "10.0.0.5:4000", nil, "198.51.100.7", "198.51.100.7"},
		{"trusted peer, no xff, x-real-ip malformed -> peer", "10.0.0.5", "10.0.0.5:4000", nil, "nope", "10.0.0.5"},
		{"trusted peer, no xff, x-real-ip trusted -> peer", "10.0.0.0/8", "10.0.0.5:4000", nil, "10.0.0.6", "10.0.0.5"},
		{"xff wins over x-real-ip", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7"}, "5.6.7.8", "198.51.100.7"},
		{"x-real-ip not used when xff has a malformed hop", "10.0.0.5", "10.0.0.5:4000", []string{"garbage"}, "5.6.7.8", "10.0.0.5"},
		{"multiple xff lines are one chain, rightmost line last hop", "10.0.0.5", "10.0.0.5:4000", []string{"6.6.6.6", "198.51.100.7"}, "", "198.51.100.7"},
		{"spaces trimmed", "10.0.0.5", "10.0.0.5:4000", []string{"  198.51.100.7  ,  10.0.0.5 "}, "", "198.51.100.7"},
		{"ipv4 with port", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7:5555"}, "", "198.51.100.7"},
		{"ipv6 client", "10.0.0.5", "10.0.0.5:4000", []string{"2001:db8::1"}, "", "2001:db8::1"},
		{"ipv6 bracket+port", "10.0.0.5", "10.0.0.5:4000", []string{"[2001:db8::1]:443"}, "", "2001:db8::1"},
		{"ipv6 bracket only", "10.0.0.5", "10.0.0.5:4000", []string{"[2001:db8::1]"}, "", "2001:db8::1"},
		{"ipv6 zone dropped", "10.0.0.5", "10.0.0.5:4000", []string{"fe80::1%eth0"}, "", "fe80::1"},
		{"ipv4-mapped ipv6 hop is unmapped", "10.0.0.5", "10.0.0.5:4000", []string{"::ffff:198.51.100.7"}, "", "198.51.100.7"},
		{"ipv4-mapped ipv6 hop matches a trusted ipv4 cidr", "10.0.0.0/8", "10.0.0.5:4000", []string{"198.51.100.7, ::ffff:10.0.0.9"}, "", "198.51.100.7"},
		{"ipv6 peer trusted by cidr", "fd00::/8", "[fd00::5]:4000", []string{"198.51.100.7"}, "", "198.51.100.7"},
		{"ipv6 peer untrusted", "10.0.0.0/8", "[2001:db8::9]:4000", []string{"198.51.100.7"}, "", "2001:db8::9"},
		{"peer without port", "10.0.0.5", "10.0.0.5", []string{"198.51.100.7"}, "", "198.51.100.7"},
		{"trusted list tolerates junk and blanks", " junk, ,10.0.0.5/99, 10.0.0.5 ", "10.0.0.5:4000", []string{"198.51.100.7"}, "", "198.51.100.7"},
		{"cidr 10.42.0.0/16 trusts a 10.42.x ingress peer", "10.42.0.0/16", "10.42.7.9:4000", []string{"6.6.6.6, 198.51.100.7"}, "", "198.51.100.7"},
		{"cidr 10.42.0.0/16 does not trust 10.43.x", "10.42.0.0/16", "10.43.7.9:4000", []string{"198.51.100.7"}, "", "10.43.7.9"},
		{"cidr boundary: just outside is untrusted", "10.0.0.0/30", "10.0.0.5:4000", []string{"198.51.100.7"}, "", "10.0.0.5"},
		{"no peer -> empty", "10.0.0.5", "", []string{"198.51.100.7"}, "", ""},
		{"non-ip peer keyed on raw text, headers ignored", "10.0.0.5", "@:0", []string{"198.51.100.7"}, "", "@"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRUSTED_PROXIES", tc.trusted)
			r := request(tc.peer, tc.xff, tc.realIP)
			r.RemoteAddr = tc.peer // NewRequest defaults; keep "" explicit
			if got := FromRequest(r); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
