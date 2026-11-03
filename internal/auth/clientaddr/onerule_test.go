package clientaddr

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

// onerule rows: (trusted list, peer, X-Forwarded-For lines, X-Real-IP) -> client. Every row is the go-api rule
// (internal/api/clientip) and the auth-service rule at once.
type oneruleRow struct {
	name    string
	trusted string
	peer    string
	xff     []string
	realIP  string
	want    string
}

var oneruleRows = []oneruleRow{
	{"C2 untrusted peer, spoofed xff and x-real-ip -> peer", "10.0.0.0/8", "203.0.113.5:4000", []string{"1.2.3.4"}, "5.6.7.8", "203.0.113.5"},
	{"C3 spoof, user, trusted web hop -> right-most untrusted", "10.0.0.0/8", "10.0.0.5:4000", []string{"9.9.9.9, 198.51.100.7, 10.0.0.9"}, "", "198.51.100.7"},
	{"C3 two header lines are one chain", "10.0.0.5", "10.0.0.5:4000", []string{"6.6.6.6", "198.51.100.7"}, "", "198.51.100.7"},
	{"C3 spoofed leftmost never read", "10.0.0.5", "10.0.0.5:4000", []string{"6.6.6.6, 198.51.100.7"}, "", "198.51.100.7"},
	{"C4 malformed right-most hop -> peer", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7, garbage"}, "", "10.0.0.5"},
	{"C4 empty hop -> peer", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7,,10.0.0.5"}, "", "10.0.0.5"},
	{"C4 ipv4 with port", "10.0.0.5", "10.0.0.5:4000", []string{"198.51.100.7:5555"}, "", "198.51.100.7"},
	{"C4 ipv6 bracket and port", "10.0.0.5", "10.0.0.5:4000", []string{"[2001:db8::1]:443"}, "", "2001:db8::1"},
	{"C4 ipv6 zone dropped", "10.0.0.5", "10.0.0.5:4000", []string{"fe80::1%eth0"}, "", "fe80::1"},
	{"C4 ipv4-mapped hop unmapped", "10.0.0.5", "10.0.0.5:4000", []string{"::ffff:198.51.100.7"}, "", "198.51.100.7"},
	{"C4 x-real-ip only when the chain is empty", "10.0.0.5", "10.0.0.5:4000", nil, "198.51.100.7", "198.51.100.7"},
	{"C4 x-real-ip only when every hop is trusted", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7"}, "198.51.100.7", "198.51.100.7"},
	{"C4 x-real-ip not used past a malformed hop", "10.0.0.5", "10.0.0.5:4000", []string{"garbage"}, "5.6.7.8", "10.0.0.5"},
	{"C4 all hops trusted, no x-real-ip -> peer", "10.0.0.0/8", "10.0.0.5:4000", []string{"10.0.0.7, 10.0.0.9"}, "", "10.0.0.5"},
	{"C5 empty trust list: xff and x-real-ip ignored", "", "10.0.0.5:4000", []string{"1.2.3.4"}, "5.6.7.8", "10.0.0.5"},
}

func oneruleRequest(row oneruleRow) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = row.peer
	for _, line := range row.xff {
		r.Header.Add("X-Forwarded-For", line)
	}
	if row.realIP != "" {
		r.Header.Set("X-Real-IP", row.realIP)
	}
	return r
}

func TestOneRuleTable(t *testing.T) {
	for _, row := range oneruleRows {
		t.Run(row.name, func(t *testing.T) {
			trust, err := ParseTrust(row.trusted)
			if err != nil {
				t.Fatal(err)
			}
			if got := trust.Resolve(oneruleRequest(row)); got != netip.MustParseAddr(row.want) {
				t.Fatalf("Resolve = %v, want %s", got, row.want)
			}
		})
	}
}

func TestEmptyTrustListParsesAndTrustsNobody(t *testing.T) {
	for _, value := range []string{"", "   "} {
		trust, err := ParseTrust(value)
		if err != nil {
			t.Fatalf("ParseTrust(%q) = %v, want no error", value, err)
		}
		if trust.Len() != 0 {
			t.Fatalf("ParseTrust(%q) trusts %d entries, want 0", value, trust.Len())
		}
	}
}

// The internal listener (service-to-service routes) binds the address with an empty Trust: a listed workload peer's
// forwarded address is never read there.
func TestEmptyTrustMiddlewareIgnoresForwardedFromAnyPeer(t *testing.T) {
	var got *string
	handler := Middleware(Trust{})(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = Text(r.Context()) }))
	handler.ServeHTTP(httptest.NewRecorder(), request("10.0.0.5:1", "198.51.100.7"))
	if got == nil || *got != "10.0.0.5" {
		t.Fatalf("Text = %v, want the peer 10.0.0.5", got)
	}
}

// An empty list trusts no address as a proxy: the walk alone cannot show it (every hop skipped still ends at the peer),
// so TrustsPeer and Contains, which the request-id and wide-trust checks read, must say no.
func TestEmptyTrustTrustsNoPeerAndContainsNothing(t *testing.T) {
	trust, err := ParseTrust("")
	if err != nil {
		t.Fatal(err)
	}
	if trust.TrustsPeer(request("10.0.0.5:4000", "198.51.100.7")) {
		t.Fatal("an empty list trusts the peer")
	}
	if trust.Contains(netip.MustParseAddr("10.0.0.5")) {
		t.Fatal("an empty list contains an address")
	}
}
