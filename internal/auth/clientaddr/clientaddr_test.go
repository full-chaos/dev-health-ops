package clientaddr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestParseTrustRefusesWhatCouldWidenIt(t *testing.T) {
	for _, value := range []string{
		"*",
		"10.0.0.1,*",
		"proxy.internal",
		"10.0.0.1,,10.0.0.2",
		"10.0.0.1/8",
		"10.0.0.0/33",
		"fe80::1%eth0",
		"::ffff:10.0.0.0/104",
		"10.0.0.1/",
	} {
		if _, err := ParseTrust(value); err == nil {
			t.Errorf("ParseTrust(%q) = nil error, want a refusal", value)
		}
	}
}

func TestParseTrustErrorsNeverEchoTheValue(t *testing.T) {
	_, err := ParseTrust("10.0.0.1,secret-host.example")
	if err == nil || strings.Contains(err.Error(), "secret-host") {
		t.Fatalf("err = %v, want a refusal that names the entry index only", err)
	}
}

func TestParseTrustAcceptsAddressesAndStrictNetworks(t *testing.T) {
	trust, err := ParseTrust(" 10.0.0.1 , 192.168.0.0/16,2001:db8::/32,::1")
	if err != nil {
		t.Fatal(err)
	}
	if trust.Len() != 4 {
		t.Fatalf("Len = %d, want 4", trust.Len())
	}
	empty, err := ParseTrust("  ")
	if err != nil || empty.Len() != 0 {
		t.Fatalf("empty value: %v, %d", err, empty.Len())
	}
}

func request(remote string, xff ...string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	r.RemoteAddr = remote
	for _, value := range xff {
		r.Header.Add("X-Forwarded-For", value)
	}
	return r
}

func TestResolve(t *testing.T) {
	trust, err := ParseTrust("10.0.0.5,192.168.0.0/16,2001:db8::/32")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		r    *http.Request
		want string
	}{
		{"untrusted peer ignores the header", request("203.0.113.9:4000", "198.51.100.7"), "203.0.113.9"},
		{"trusted peer without the header", request("10.0.0.5:4000"), "10.0.0.5"},
		{"trusted peer, one value", request("10.0.0.5:4000", "198.51.100.7"), "198.51.100.7"},
		{"trusted network peer", request("192.168.4.4:1", " 198.51.100.7 "), "198.51.100.7"},
		{"trusted IPv6 peer, IPv6 client", request("[2001:db8::9]:1", "2001:db8:ffff::1"), "2001:db8:ffff::1"},
		{"a chain is not one value", request("10.0.0.5:4000", "198.51.100.7, 10.0.0.9"), "10.0.0.5"},
		{"two header lines", request("10.0.0.5:4000", "198.51.100.7", "198.51.100.8"), "10.0.0.5"},
		{"a value that is not an address", request("10.0.0.5:4000", "unknown"), "10.0.0.5"},
		{"a value with a port", request("10.0.0.5:4000", "198.51.100.7:80"), "10.0.0.5"},
		{"a zoned value", request("10.0.0.5:4000", "fe80::1%eth0"), "10.0.0.5"},
		{"an empty value", request("10.0.0.5:4000", ""), "10.0.0.5"},
		{"a mapped IPv4 peer is IPv4", request("[::ffff:10.0.0.5]:4000", "198.51.100.7"), "198.51.100.7"},
		{"a mapped IPv4 client is IPv4", request("10.0.0.5:4000", "::ffff:198.51.100.7"), "198.51.100.7"},
		{"an address outside the network", request("192.169.0.1:1", "198.51.100.7"), "192.169.0.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := trust.Resolve(c.r)
			if got != netip.MustParseAddr(c.want) {
				t.Fatalf("Resolve = %v, want %s", got, c.want)
			}
		})
	}
}

func TestResolveWithNoTrustIsAlwaysThePeer(t *testing.T) {
	trust, err := ParseTrust("")
	if err != nil {
		t.Fatal(err)
	}
	if got := trust.Resolve(request("127.0.0.1:1", "198.51.100.7")); got != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("Resolve = %v, want the peer", got)
	}
}

func TestMiddlewareBindsTheAddress(t *testing.T) {
	trust, err := ParseTrust("10.0.0.5")
	if err != nil {
		t.Fatal(err)
	}
	var got *string
	handler := Middleware(trust)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = Text(r.Context()) }))
	handler.ServeHTTP(httptest.NewRecorder(), request("10.0.0.5:1", "198.51.100.7"))
	if got == nil || *got != "198.51.100.7" {
		t.Fatalf("Text = %v, want 198.51.100.7", got)
	}
	got = nil
	handler.ServeHTTP(httptest.NewRecorder(), request("not-an-address", "198.51.100.7"))
	if got != nil {
		t.Fatalf("Text = %q for an unparsable peer, want nil", *got)
	}
	if Text(context.Background()) != nil {
		t.Fatal("Text of an empty context is not nil")
	}
}
