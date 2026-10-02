package llmorgsettings

import (
	"context"
	"net/netip"
	"testing"
)

// These tests pin, clause by clause, what isSafePublicIP refuses: each address range of extraUnsafePrefixes that no other
// test names, the unwrapping of an IPv4-mapped IPv6 address, and the multicast scopes that are not link-local. A case
// is a blocked address (the first, the last and an inner address of a range) and the closest address outside the range,
// which must still be accepted: a clause that is widened, narrowed or dropped turns one of them red. Addresses are
// literals; no name is resolved and no network call is made.

type clauseCase struct {
	name    string
	blocked []string
	allowed []string // the nearest addresses outside the clause that no other clause refuses
}

func TestIsSafePublicIPRefusesEachSpecialPurposeRange(t *testing.T) {
	for _, c := range []clauseCase{
		{"100.64.0.0/10 shared address space (CGNAT)", []string{"100.64.0.0", "100.100.0.1", "100.127.255.255"}, []string{"100.63.255.255", "100.128.0.0"}},
		{"192.0.2.0/24 documentation (TEST-NET-1)", []string{"192.0.2.0", "192.0.2.200", "192.0.2.255"}, []string{"192.0.1.255", "192.0.3.0"}},
		{"192.0.0.0/24 IETF protocol assignments", []string{"192.0.0.0", "192.0.0.170", "192.0.0.255"}, []string{"191.255.255.255", "192.0.1.0"}},
		{"192.88.99.0/24 6to4 relay anycast", []string{"192.88.99.0", "192.88.99.1", "192.88.99.255"}, []string{"192.88.98.255", "192.88.100.0"}},
		{"198.18.0.0/15 benchmarking", []string{"198.18.0.0", "198.18.255.1", "198.19.255.255"}, []string{"198.17.255.255", "198.20.0.0"}},
		{"198.51.100.0/24 documentation (TEST-NET-2)", []string{"198.51.100.0", "198.51.100.77", "198.51.100.255"}, []string{"198.51.99.255", "198.51.101.0"}},
		{"203.0.113.0/24 documentation (TEST-NET-3)", []string{"203.0.113.0", "203.0.113.77", "203.0.113.255"}, []string{"203.0.112.255", "203.0.114.0"}},
		{"2001:db8::/32 documentation (IPv6)", []string{"2001:db8::", "2001:db8:1234::1", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"}, []string{"2001:db7:ffff::1", "2001:db9::1"}},
		{"240.0.0.0/4 reserved", []string{"240.0.0.0", "248.1.2.3", "255.255.255.254"}, []string{"223.255.255.255"}},
	} {
		t.Run(c.name, func(t *testing.T) { assertClause(t, c) })
	}
}

func TestIsSafePublicIPUnwrapsAnIPv4MappedAddressBeforeJudgingIt(t *testing.T) {
	assertClause(t, clauseCase{
		name: "IPv4-mapped IPv6",
		// Each blocked address is a range the v4 form is refused for only because the mapped address is unwrapped:
		// a v6 wrapper is never inside a v4 prefix, so without the unwrap these would pass.
		blocked: []string{"::ffff:100.64.0.1", "::ffff:192.0.2.9", "::ffff:198.18.0.1", "::ffff:240.0.0.1", "::ffff:255.255.255.255"},
		allowed: []string{"::ffff:8.8.8.8", "::ffff:93.184.216.34"},
	})
}

func TestIsSafePublicIPRefusesMulticastBeyondTheLinkLocalScopes(t *testing.T) {
	assertClause(t, clauseCase{
		name: "multicast",
		// 224.0.0.0/24 (link-local) and ff02::/16, ff01::/16 have their own clauses; these are the scopes only the
		// general multicast clause refuses.
		blocked: []string{"224.0.1.1", "232.1.2.3", "239.1.1.1", "239.255.255.255", "ff0e::1", "ff05::1", "ff08::1"},
		allowed: []string{"223.255.255.255", "2606:4700:4700::1111"},
	})
}

func TestIsSafePublicIPRefusesTheUnspecifiedAndTheInvalidAddress(t *testing.T) {
	assertClause(t, clauseCase{name: "unspecified", blocked: []string{"0.0.0.0", "::"}, allowed: []string{"1.0.0.1", "2606:4700:4700::1111"}})
	t.Run("refuses the zero netip.Addr", func(t *testing.T) {
		if isSafePublicIP(netip.Addr{}) {
			t.Fatal("an invalid address was accepted as a safe public address")
		}
	})
}

// TestValidateBaseURLRefusesTheSameClausesThroughTheURLPath goes through validateBaseURL with a stub resolver, so the
// literal-address check and the https rule are the real ones.
func TestValidateBaseURLRefusesTheSameClausesThroughTheURLPath(t *testing.T) {
	for url, want := range map[string]bool{
		"https://192.0.0.9/v1":           false,
		"https://192.0.1.1/v1":           true,
		"https://198.19.0.1/v1":          false,
		"https://198.20.0.1/v1":          true,
		"https://[::ffff:100.64.0.1]/v1": false,
		"https://[::ffff:8.8.8.8]/v1":    true,
		"https://239.1.1.1/v1":           false,
		"https://[ff0e::1]/v1":           false,
		"https://240.0.0.7/v1":           false,
	} {
		t.Run(url, func(t *testing.T) {
			ok, reason := validateBaseURLText(context.Background(), url, hermeticResolver)
			if ok != want {
				t.Fatalf("validateBaseURL(%q) ok=%v reason=%q, want ok=%v", url, ok, reason, want)
			}
			if !ok && reason == "" {
				t.Fatalf("a refusal of %q carries no reason", url)
			}
		})
	}
}

func assertClause(t *testing.T, c clauseCase) {
	t.Helper()
	for _, text := range c.blocked {
		t.Run("refuses "+text, func(t *testing.T) {
			addr := netip.MustParseAddr(text)
			if isSafePublicIP(addr) {
				t.Fatalf("%s (%s) was accepted as a safe public address", text, c.name)
			}
		})
	}
	for _, text := range c.allowed {
		t.Run("accepts "+text, func(t *testing.T) {
			addr := netip.MustParseAddr(text)
			if !isSafePublicIP(addr) {
				t.Fatalf("%s, next to %s, was refused: the clause reaches past its range", text, c.name)
			}
		})
	}
}
