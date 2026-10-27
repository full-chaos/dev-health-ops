package clientip

import "testing"

// The node pod range holds the node's own addresses (network address 10.42.0.0, bridge 10.42.0.1): a host process or
// a hostNetwork pod reaches go-api from them, so "!<address>" entries take them out of the trusted range.
func TestFromRequestNeverTrustsAnExcludedPeer(t *testing.T) {
	const list = "10.42.0.0/24,!10.42.0.0,!10.42.0.1"
	cases := []struct {
		name, peer string
		xff        []string
		realIP     string
		want       string
	}{
		{"excluded network address: xff ignored", "10.42.0.0:4000", []string{"203.0.113.9"}, "", "10.42.0.0"},
		{"excluded bridge: xff ignored", "10.42.0.1:4000", []string{"203.0.113.9"}, "", "10.42.0.1"},
		{"excluded bridge: x-real-ip ignored", "10.42.0.1:4000", nil, "203.0.113.9", "10.42.0.1"},
		{"excluded mapped bridge: xff ignored", "[::ffff:10.42.0.1]:4000", []string{"203.0.113.9"}, "", "10.42.0.1"},
		{"ingress pod in the range: xff believed", "10.42.0.7:4000", []string{"203.0.113.9"}, "", "203.0.113.9"},
		{"excluded hop is the client, never skipped", "10.42.0.7:4000", []string{"6.6.6.6, 10.42.0.1"}, "", "10.42.0.1"},
		{"trusted hop skipped up to the client", "10.42.0.7:4000", []string{"6.6.6.6, 203.0.113.9, 10.42.0.9"}, "", "203.0.113.9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRUSTED_PROXIES", list)
			if got := FromRequest(request(tc.peer, tc.xff, tc.realIP)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A list go-api refuses at start trusts nobody if it is ever read on a request: no entry of it is believed.
func TestFromRequestTrustsNobodyOnABadList(t *testing.T) {
	for _, list := range []string{
		"10.42.0.0/24,bogus", "10.42.0.0/24,", "10.42.0.0/24,*", "0.0.0.0/0", "::/0", "::ffff:10.42.0.0/120",
		"::ffff:10.42.0.7", "10.42.0.5/24", "10.42.0.0/24,!10.43.0.1", "10.42.0.0/24,!10.42.0.0/31", "10.42.0.7%eth0",
	} {
		t.Run(list, func(t *testing.T) {
			t.Setenv("TRUSTED_PROXIES", list)
			if got := FromRequest(request("10.42.0.7:4000", []string{"203.0.113.9"}, "198.51.100.1")); got != "10.42.0.7" {
				t.Fatalf("got %q, want the peer 10.42.0.7", got)
			}
		})
	}
}
