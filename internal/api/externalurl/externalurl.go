// Package externalurl is the SSRF guard the credential and team admin routes
// apply to a stored provider URL before any outbound call.
package externalurl

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

// Validate ports _validate_external_url
// (src/dev_health_ops/api/admin/routers/credentials.py:466-501), the SSRF
// guard Python's discover route applies to a GitHub App credential's
// base_url before minting an installation token against it. It returns the
// same (ok, error-detail) pair; detail is "" when ok.
//
// The IP classification tables below are copied from CPython 3.14's
// ipaddress module (`_private_networks`, `_private_networks_exceptions`,
// `_reserved_network(s)`, link-local) rather than approximated from Go's
// net/netip predicates, which cover a strict subset of them (netip
// IsPrivate is RFC 1918 / fc00::/7 only).
func Validate(ctx context.Context, rawURL string, lookup func(context.Context, string) ([]netip.Addr, error)) (bool, string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// urlparse never raises for these inputs; an unparseable value has
		// no valid scheme, which is the first check.
		return false, "Invalid URL scheme - only http and https are allowed"
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false, "Invalid URL scheme - only http and https are allowed"
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return false, "No hostname in URL"
	}
	// Python tests truthiness (`parsed.username or parsed.password`), so an
	// empty userinfo ("http://@host") is allowed there; mirror that.
	if parsed.User != nil {
		password, _ := parsed.User.Password()
		if parsed.User.Username() != "" || password != "" {
			return false, "Credentials must not be embedded in the URL"
		}
	}
	switch strings.ToLower(hostname) {
	case "localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]":
		return false, "Connection to localhost is not allowed"
	}
	addrs, err := lookup(ctx, hostname)
	if err != nil {
		return false, "Cannot resolve hostname: " + hostname
	}
	for _, addr := range addrs {
		if pythonIPNotGlobalTarget(addr) {
			return false, "Connection to private/internal networks is not allowed"
		}
	}
	return true, ""
}

// ResolveHostAddrs is the system resolver, the lookup Validate uses in
// production.
func ResolveHostAddrs(ctx context.Context, host string) ([]netip.Addr, error) {
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		if addr, ok := netip.AddrFromSlice(ip.IP); ok {
			out = append(out, addr)
		}
	}
	return out, nil
}

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, cidr := range cidrs {
		out[i] = netip.MustParsePrefix(cidr)
	}
	return out
}

var (
	pyV4Private = mustPrefixes("0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
		"192.0.0.0/24", "192.0.0.170/31", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15",
		"198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "255.255.255.255/32")
	pyV4PrivateExceptions = mustPrefixes("192.0.0.9/32", "192.0.0.10/32")
	pyV4Reserved          = mustPrefixes("240.0.0.0/4")
	pyV4LinkLocal         = mustPrefixes("169.254.0.0/16")
	pyV4Loopback          = mustPrefixes("127.0.0.0/8")

	pyV6Private = mustPrefixes("::1/128", "::/128", "::ffff:0:0/96", "64:ff9b:1::/48", "100::/64", "2001::/23",
		"2001:db8::/32", "2002::/16", "3fff::/20", "fc00::/7", "fe80::/10")
	pyV6PrivateExceptions = mustPrefixes("2001:1::1/128", "2001:1::2/128", "2001:3::/32", "2001:4:112::/48",
		"2001:20::/28", "2001:30::/28")
	pyV6Reserved = mustPrefixes("::/8", "100::/8", "200::/7", "400::/6", "800::/5", "1000::/4", "4000::/3",
		"6000::/3", "8000::/3", "a000::/3", "c000::/3", "e000::/4", "f000::/5", "f800::/6", "fe00::/9")
	pyV6LinkLocal = mustPrefixes("fe80::/10")
	pyV6Loopback  = mustPrefixes("::1/128")
)

func inAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// pythonIPNotGlobalTarget is `ip.is_private or ip.is_loopback or
// ip.is_link_local or ip.is_reserved` for one resolved address.
//
// An IPv4-mapped IPv6 address (::ffff:a.b.c.d) is classified by its embedded
// IPv4 address, as CPython 3.13+ does -- and Go's resolver hands back plain
// IPv4 as 16-byte "4in6" values, so unmapping first is also what keeps every
// public IPv4 address from landing in the ::ffff:0:0/96 private table (the
// first version of this port rejected ALL public IPv4; the venue oracle
// caught it on the first run).
func pythonIPNotGlobalTarget(addr netip.Addr) bool {
	addr = addr.Unmap()
	if addr.Is4() {
		private := inAny(addr, pyV4Private) && !inAny(addr, pyV4PrivateExceptions)
		return private || inAny(addr, pyV4Loopback) || inAny(addr, pyV4LinkLocal) || inAny(addr, pyV4Reserved)
	}
	private := inAny(addr, pyV6Private) && !inAny(addr, pyV6PrivateExceptions)
	return private || inAny(addr, pyV6Loopback) || inAny(addr, pyV6LinkLocal) || inAny(addr, pyV6Reserved)
}
