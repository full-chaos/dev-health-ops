// Package clientip is the one place the Go api derives "who is the client"
// from a request, for rate-limit keys and audit rows (CHAOS-7204).
//
// The rule (a bypass here is a rate-limit or audit-spoofing bug):
//
//  1. The client is the TCP peer, unless the peer is a trusted proxy. An
//     untrusted peer's forwarding headers are ignored entirely.
//  2. Trusted proxies come from TRUSTED_PROXIES (comma-separated IPs and/or
//     CIDRs), read on every call. Unset or empty trusts nobody (fail closed).
//  3. For a trusted peer, X-Forwarded-For (all header lines joined) is walked
//     from the RIGHT: each hop a trusted proxy appended is skipped, and the
//     first untrusted hop is the client. Entries left of that hop are
//     client-supplied and are never read, so a spoofed leftmost entry cannot
//     change the result whatever the ingress does with the header.
//  4. A malformed entry reached during the walk never wins: the walk stops and
//     the peer is used.
//  5. When the trusted peer sent no X-Forwarded-For, or every hop in it was
//     trusted, a valid untrusted X-Real-IP is the client; else the peer.
//
// Trust contract (r1 finding, CHAOS-7204): a proxy listed in TRUSTED_PROXIES must
// APPEND its peer to X-Forwarded-For (ingress-nginx compute-full-forwarded-for=true)
// or REPLACE the header with that peer (compute-full-forwarded-for=false, no
// use-forwarded-headers). Either way the rightmost hop is written by the proxy,
// not the client. A proxy that forwards a client-written header UNCHANGED
// (use-forwarded-headers=true with compute-full-forwarded-for=false, in front of
// nothing that appends) does not meet the contract: no peer-based rule can tell
// its client-written last hop from a real one, so do not list such a proxy.
//
// Addresses come back canonical (IPv4-mapped IPv6 unmapped, zone, port and
// brackets dropped) so one client cannot split across several bucket keys.
package clientip

import (
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
)

// FromRequest returns the client address per the package rule, or "" when the
// request carries no peer address at all.
func FromRequest(r *http.Request) string {
	peerText := peerHost(r.RemoteAddr)
	peer, peerOK := parseAddr(peerText)
	if !peerOK {
		// Not an IP (unix socket, test harness): nothing to trust, key on the
		// raw text so distinct peers stay distinct.
		return peerText
	}
	trusted := parseTrusted(os.Getenv("TRUSTED_PROXIES"))
	if !trusted.contains(peer) {
		return peer.String()
	}
	if forwarded := strings.Join(r.Header.Values("X-Forwarded-For"), ","); strings.TrimSpace(forwarded) != "" {
		hops := strings.Split(forwarded, ",")
		for i := len(hops) - 1; i >= 0; i-- {
			hop, ok := parseAddr(hops[i])
			if !ok {
				return peer.String()
			}
			if !trusted.contains(hop) {
				return hop.String()
			}
		}
	}
	if real, ok := parseAddr(r.Header.Get("X-Real-IP")); ok && !trusted.contains(real) {
		return real.String()
	}
	return peer.String()
}

func peerHost(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}

// parseAddr reads one address as a proxy header or peer carries it: bare IP,
// IP:port, [IPv6] or [IPv6]:port, surrounding spaces ignored. IPv4-mapped
// IPv6 is unmapped and any zone dropped.
func parseAddr(text string) (netip.Addr, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return netip.Addr{}, false
	}
	if addr, err := netip.ParseAddr(text); err == nil {
		return canonical(addr), true
	}
	if ap, err := netip.ParseAddrPort(text); err == nil {
		return canonical(ap.Addr()), true
	}
	if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
		if addr, err := netip.ParseAddr(text[1 : len(text)-1]); err == nil {
			return canonical(addr), true
		}
	}
	return netip.Addr{}, false
}

func canonical(addr netip.Addr) netip.Addr { return addr.Unmap().WithZone("") }

type trustedSet struct {
	prefixes []netip.Prefix
}

func parseTrusted(raw string) trustedSet {
	var set trustedSet
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "/") {
			if prefix, err := netip.ParsePrefix(part); err == nil {
				set.prefixes = append(set.prefixes, netip.PrefixFrom(canonical(prefix.Addr()), unmapBits(prefix)).Masked())
			}
			continue
		}
		if addr, err := netip.ParseAddr(part); err == nil {
			addr = canonical(addr)
			set.prefixes = append(set.prefixes, netip.PrefixFrom(addr, addr.BitLen()))
		}
	}
	return set
}

// unmapBits keeps an IPv4-mapped IPv6 prefix (::ffff:a.b.c.d/96+n) meaning the
// same IPv4 range once its address is unmapped.
func unmapBits(prefix netip.Prefix) int {
	if prefix.Addr().Is4In6() && prefix.Bits() >= 96 {
		return prefix.Bits() - 96
	}
	return prefix.Bits()
}

func (s trustedSet) contains(addr netip.Addr) bool {
	for _, prefix := range s.prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}
