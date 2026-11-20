package config

import (
	"net/netip"
	"net/url"
	"strings"
	"unicode"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/churl"
)

const (
	hostProblemNoUsableHost     = "no_usable_host"
	hostProblemEmptyHostElement = "empty_host_element"
)

// ClickHouseHostProblem names a service DSN key whose address list loads but holds an address the driver cannot dial.
type ClickHouseHostProblem struct {
	Key   string
	Cause string
}

// parseClickHouseDSN reads a DSN the way clickhouse-go does (churl accepts address lists in the host part). A DSN that
// net/url accepts loads as before, so no form that loaded before is refused. A DSN only churl reads (an address list)
// must also be accepted by the driver's full ParseDSN, so a bad option on a list is refused as at base.
func parseClickHouseDSN(raw string) (*url.URL, bool) {
	legacy, legacyErr := url.Parse(raw)
	churlParsed, churlErr := churl.Parse(raw)
	if legacyErr == nil && legacy != nil {
		if churlErr == nil && churlParsed != nil {
			return churlParsed, true
		}
		return legacy, true
	}
	if churlErr != nil || churlParsed == nil {
		return nil, false
	}
	if _, err := clickhouse.ParseDSN(raw); err != nil {
		return nil, false
	}
	return churlParsed, true
}

// clickHouseAddrs is the address list clickhouse-go ParseDSN builds: hosts= first, then the host part split on commas,
// then alt_hosts=. parseHostList in clickhouse-go is unexported, so its trim-and-drop-empty rule is mirrored here.
func clickHouseAddrs(parsed *url.URL) []string {
	params := parsed.Query()
	addrs := hostListParam(params.Get("hosts"))
	addrs = append(addrs, strings.Split(parsed.Host, ",")...)
	return append(addrs, hostListParam(params.Get("alt_hosts"))...)
}

func hostListParam(value string) []string {
	var hosts []string
	for _, host := range strings.Split(value, ",") {
		if host = strings.TrimSpace(host); host != "" {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// addrHasUsableHost reports whether one host[:port] address is well formed and names a host: a bracketed IP literal
// (nothing after "]" but an optional :port), or a name with a letter or digit and at most one colon. The parsed host
// part is already percent-decoded, so an IPv6 zone is passed to netip as it stands.
func addrHasUsableHost(addr string) bool {
	var host, port string
	if strings.HasPrefix(addr, "[") {
		end := strings.Index(addr, "]")
		if end < 0 {
			return false
		}
		host, port = addr[1:end], addr[end+1:]
		if port != "" && !strings.HasPrefix(port, ":") {
			return false
		}
		port = strings.TrimPrefix(port, ":")
		if _, err := netip.ParseAddr(host); err != nil {
			return false
		}
		return portDigits(port)
	}
	if strings.Count(addr, ":") > 1 {
		return false
	}
	host, port, _ = strings.Cut(addr, ":")
	if !portDigits(port) {
		return false
	}
	return strings.ContainsFunc(host, func(r rune) bool {
		return unicode.IsLetter(r) || unicode.IsDigit(r)
	})
}

func portDigits(port string) bool {
	for _, r := range port {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// clickHouseHostProblem returns "" when every address names a host, "no_usable_host" when none does, else
// "empty_host_element".
func clickHouseHostProblem(parsed *url.URL) string {
	return hostProblemOf(clickHouseAddrs(parsed))
}

// clickHouseAuthorityHostProblem is clickHouseHostProblem over the host part only. The http and https gate uses it:
// a bad hosts= or alt_hosts= value loaded at base and still loads, with a WARN.
func clickHouseAuthorityHostProblem(parsed *url.URL) string {
	return hostProblemOf(strings.Split(parsed.Host, ","))
}

func hostProblemOf(addrs []string) string {
	usable, total := 0, 0
	for _, addr := range addrs {
		total++
		if addrHasUsableHost(addr) {
			usable++
		}
	}
	switch {
	case usable == 0:
		return hostProblemNoUsableHost
	case usable < total:
		return hostProblemEmptyHostElement
	}
	return ""
}
