package llmorgsettings

import (
	"context"
	"net"
	"net/netip"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyidna"
)

// resolver looks up the IP addresses a hostname resolves to, mirroring
// Python's socket.getaddrinfo call inside validate_llm_base_url's
// _resolved_addresses. Injectable so tests can supply a hermetic table
// instead of hitting real DNS -- the same shape as
// tests/test_byo_base_url_ssrf.py's own `hermetic_dns` fixture. A resolver
// error (including "no such host") is treated as "unresolvable", not
// "unsafe" -- see validateBaseURL's final resolve step.
type resolver func(ctx context.Context, host string) ([]net.IP, error)

func defaultResolver(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, addr := range addrs {
		ips = append(ips, addr.IP)
	}
	return ips, nil
}

// ValidateBaseURL ports llm/credentials.py's validate_llm_base_url
// verbatim (CHAOS-2552's best-effort app-layer SSRF guard for BYO LLM
// base_url): an empty base_url is allowed (the provider SDK default
// applies). Otherwise the URL must be http(s) with no userinfo/control
// characters, must resolve only to safe public targets, and must use
// https unless it is CURRENTLY unresolvable -- an unresolvable name is not
// treated as an SSRF target at persist time (DNS TOCTOU is deferred to
// runtime re-validation plus network egress filtering, matching Python's
// own comment on _resolved_addresses' empty-set branch). ok=true,
// reason="" on success; ok=false, reason=<non-empty> on rejection.
func ValidateBaseURL(ctx context.Context, baseURL string) (bool, string) {
	ok, reason, err := ValidateBaseURLChecked(ctx, baseURL)
	if err != nil {
		return false, err.Error()
	}
	return ok, reason
}

// ValidateBaseURLChecked is ValidateBaseURL with Python's third outcome: the
// error is the ValueError urllib.parse.urlsplit raises for a malformed
// netloc (an unbalanced or invalid IPv6 bracket), which validate_llm_base_url
// does not catch -- a caller that answers as the Python api does answers its
// unhandled-error response.
func ValidateBaseURLChecked(ctx context.Context, baseURL string) (bool, string, error) {
	return validateBaseURL(ctx, baseURL, defaultResolver)
}

func validateBaseURL(ctx context.Context, baseURL string, resolve resolver) (bool, string, error) {
	if baseURL == "" {
		return true, "", nil
	}
	if containsControlOrSpace(baseURL) {
		return false, "LLM base_url must not contain whitespace or control characters", nil
	}
	parsed, err := pythonparity.SplitURL(baseURL)
	if err != nil {
		return false, "", err
	}
	if parsed.HasUserInfo() {
		return false, "LLM base_url must not include userinfo", nil
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false, "LLM base_url must use http or https", nil
	}
	rawHost, hasHost := parsed.Hostname()
	_, _, portErr := parsed.Port()
	if portErr != nil {
		return false, "LLM base_url is invalid: " + portErr.Error(), nil
	}
	if !hasHost {
		return false, "LLM base_url is missing a host", nil
	}
	host, reason := normalizeHost(rawHost)
	if reason != "" {
		return false, reason, nil
	}
	if parsed.Scheme != "https" {
		return false, "LLM base_url must use https", nil
	}

	if literal, ok := pythonparity.ParseIPAddress(host); ok {
		if !isSafePublicIP(literal) {
			return false, "LLM base_url host resolves to a non-public address", nil
		}
	}

	addresses, err := resolve(ctx, host)
	if err != nil || len(addresses) == 0 {
		// Unresolvable names are not SSRF targets at persist-time -- see
		// this function's doc comment.
		return true, "", nil
	}
	for _, addr := range addresses {
		ip, ok := netip.AddrFromSlice(addr)
		if !ok {
			continue
		}
		if !isSafePublicIP(ip) {
			return false, "LLM base_url host resolves to a non-public address", nil
		}
	}
	return true, "", nil
}

func containsControlOrSpace(value string) bool {
	for _, r := range value {
		if r <= 0x20 || r == 0x7F {
			return true
		}
	}
	return false
}

// normalizeHost ports _normalize_url_host: strip a trailing dot, lowercase,
// and IDNA-encode -- except the literal string "localhost", left alone
// (matching Python's own special case, which exists there to skip an
// encode() call that would otherwise succeed unchanged anyway).
func normalizeHost(host string) (string, string) {
	stripped := strings.TrimRight(host, ".")
	if stripped == "" {
		return "", "LLM base_url is missing a host"
	}
	lowered := pythonparity.Lower(stripped)
	if lowered == "localhost" {
		return lowered, ""
	}
	normalized, err := pyidna.CodecEncode(lowered)
	if err != nil {
		return "", "LLM base_url host is not valid IDNA"
	}
	return normalized, ""
}

// The address test below is the port of llm/credentials.py _ip_is_safe_public_target: an address is a safe public target
// when Python's ipaddress says is_global and not loopback, private, link-local, multicast, unspecified or reserved.
// netip's helpers cover loopback, private (10/8, 172.16/12, 192.168/16, fc00::/7), link-local, multicast and
// unspecified; the tables below are the rest, taken from CPython 3.14's Lib/ipaddress.py (the interpreter the
// recording in testdata/golden/ssrf_address_classes.json was made on) so the list is complete by construction, not by the
// cases a review found: IPv4Address._private_networks (0.0.0.0/8, 192.0.0.0/24, 192.0.2.0/24, 198.18.0.0/15,
// 198.51.100.0/24, 203.0.113.0/24, 240.0.0.0/4, 255.255.255.255/32, ...) and the 100.64.0.0/10 carve-out of
// IPv4Address.is_global; IPv6Address._private_networks with its _private_networks_exceptions; IPv6Address._reserved_networks.
//
// Three ranges are stricter than the reference ON PURPOSE (a pinned Known, ssrf_address_classes_test.go knownStricter): fec0::/10 (deprecated site-local, internal use) and the
// whole of 192.0.0.0/24 (the reference refuses only 192.0.0.0/29 and 192.0.0.170/31 and accepts 192.0.0.9 and 192.0.0.10,
// the RFC 7600 and RFC 8155 globally reachable addresses) and 192.88.99.0/24 (RFC 7526 6to4 relay anycast, deprecated).

// nonGlobalV4 are the IPv4 ranges that are not globally reachable and that no netip helper covers.
var nonGlobalV4 = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
}

// nonGlobalV6 are IPv6Address._private_networks that no netip helper covers (::1 and :: are loopback and unspecified,
// ::ffff:0:0/96 is unwrapped to its IPv4 form first, fc00::/7 is private, fe80::/10 is link-local).
var nonGlobalV6 = []netip.Prefix{
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	// Stricter than the reference on purpose (a pinned Known): the deprecated site-local range fec0::/10, which the
	// reference accepts. It is internal-use address space (RFC 3879).
	netip.MustParsePrefix("fec0::/10"),
}

// globalExceptionsV6 are IPv6Address._private_networks_exceptions: inside a range of nonGlobalV6 yet globally reachable.
var globalExceptionsV6 = []netip.Prefix{
	netip.MustParsePrefix("2001:1::1/128"),
	netip.MustParsePrefix("2001:1::2/128"),
	netip.MustParsePrefix("2001:3::/32"),
	netip.MustParsePrefix("2001:4:112::/48"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:30::/28"),
}

// reservedV6 are IPv6Address._reserved_networks: everything outside 2000::/3 that is not another class above.
var reservedV6 = []netip.Prefix{
	netip.MustParsePrefix("::/8"), netip.MustParsePrefix("100::/8"), netip.MustParsePrefix("200::/7"),
	netip.MustParsePrefix("400::/6"), netip.MustParsePrefix("800::/5"), netip.MustParsePrefix("1000::/4"),
	netip.MustParsePrefix("4000::/3"), netip.MustParsePrefix("6000::/3"), netip.MustParsePrefix("8000::/3"),
	netip.MustParsePrefix("a000::/3"), netip.MustParsePrefix("c000::/3"), netip.MustParsePrefix("e000::/4"),
	netip.MustParsePrefix("f000::/5"), netip.MustParsePrefix("f800::/6"), netip.MustParsePrefix("fe00::/9"),
}

func inAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// isSafePublicIP mirrors _ip_is_safe_public_target: a v4-mapped v6 address
// is unwrapped to its v4 form first (Python does the equivalent via
// ipv4_mapped) so an SSRF target hidden behind ::ffff:<v4> is judged by
// its real v4 address, not its (structurally always "global") v6 wrapper.
func isSafePublicIP(addr netip.Addr) bool {
	if !addr.IsValid() {
		return false
	}
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() || addr.IsInterfaceLocalMulticast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	if addr.Is4() {
		return !inAny(addr, nonGlobalV4)
	}
	if inAny(addr, reservedV6) {
		return false
	}
	return !inAny(addr, nonGlobalV6) || inAny(addr, globalExceptionsV6)
}
