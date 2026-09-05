package llmorgsettings

import (
	"context"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
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
	return validateBaseURL(ctx, baseURL, defaultResolver)
}

func validateBaseURL(ctx context.Context, baseURL string, resolve resolver) (bool, string) {
	if baseURL == "" {
		return true, ""
	}
	if containsControlOrSpace(baseURL) {
		return false, "LLM base_url must not contain whitespace or control characters"
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return false, "LLM base_url is invalid: " + err.Error()
	}
	if parsed.User != nil {
		return false, "LLM base_url must not include userinfo"
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return false, "LLM base_url must use http or https"
	}
	rawHost := parsed.Hostname()
	if rawHost == "" {
		return false, "LLM base_url is missing a host"
	}
	// codex round 3, P2: Go's net/url only checks a port is ALL-DIGIT
	// syntax (net/url's validOptionalPort), never its magnitude -- unlike
	// Python's urlsplit.port, which raises ValueError("Port out of range
	// 0-65535") for e.g. ":65536" (credentials.py:182, caught by
	// validate_llm_base_url's own try/except ValueError and rejected).
	// Without this check Go accepted a URL like
	// "https://host:65536/v1" as a usable BYO endpoint where Python
	// rejects it outright.
	if portStr := parsed.Port(); portStr != "" {
		port, perr := strconv.Atoi(portStr)
		if perr != nil || port < 0 || port > 65535 {
			return false, "LLM base_url is invalid: port out of range 0-65535"
		}
	}
	host, reason := normalizeHost(rawHost)
	if reason != "" {
		return false, reason
	}
	if scheme != "https" {
		return false, "LLM base_url must use https"
	}

	if literal, err := netip.ParseAddr(host); err == nil {
		if !isSafePublicIP(literal) {
			return false, "LLM base_url host resolves to a non-public address"
		}
	}

	addresses, err := resolve(ctx, host)
	if err != nil || len(addresses) == 0 {
		// Unresolvable names are not SSRF targets at persist-time -- see
		// this function's doc comment.
		return true, ""
	}
	for _, addr := range addresses {
		ip, ok := netip.AddrFromSlice(addr)
		if !ok {
			continue
		}
		if !isSafePublicIP(ip) {
			return false, "LLM base_url host resolves to a non-public address"
		}
	}
	return true, ""
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
	lowered := strings.ToLower(stripped)
	if lowered == "localhost" {
		return lowered, ""
	}
	normalized, err := idna.ToASCII(lowered)
	if err != nil {
		return "", "LLM base_url host is not valid IDNA"
	}
	return normalized, ""
}

// extraUnsafePrefixes are IANA special-purpose ranges Go's netip.Addr
// helpers (IsPrivate/IsLoopback/IsLinkLocalUnicast/IsMulticast/
// IsUnspecified) do not cover on their own, mirroring the ranges Python's
// ipaddress.IPv4Address/IPv6Address.is_global excludes beyond RFC1918/
// loopback/link-local/multicast/unspecified (CGNAT, documentation/
// benchmarking/reserved blocks, the IPv4 limited broadcast address, and
// the IPv6 documentation prefix).
var extraUnsafePrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
	netip.MustParsePrefix("2001:db8::/32"),
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
	for _, prefix := range extraUnsafePrefixes {
		if prefix.Addr().Is4() == addr.Is4() && prefix.Contains(addr) {
			return false
		}
	}
	return true
}
