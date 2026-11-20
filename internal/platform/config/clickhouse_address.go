package config

import (
	"net/netip"
	"net/url"
	"strings"
	"unicode"

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

// parseClickHouseDSN reads a DSN the way clickhouse-go does (churl accepts address lists in the host part). A DSN
// churl refuses but net/url accepts keeps loading, so no form that loaded before is refused now.
func parseClickHouseDSN(raw string) (*url.URL, bool) {
	if parsed, err := churl.Parse(raw); err == nil && parsed != nil {
		return parsed, true
	}
	if parsed, err := url.Parse(raw); err == nil && parsed != nil {
		return parsed, true
	}
	return nil, false
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

// addrHasUsableHost reports whether one host[:port] address names a host: a bracketed IP literal, or a name with a
// letter or digit.
func addrHasUsableHost(addr string) bool {
	if strings.HasPrefix(addr, "[") {
		end := strings.Index(addr, "]")
		if end < 0 {
			return false
		}
		_, err := netip.ParseAddr(strings.Replace(addr[1:end], "%25", "%", 1))
		return err == nil
	}
	host := addr
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
	}
	return strings.ContainsFunc(host, func(r rune) bool {
		return unicode.IsLetter(r) || unicode.IsDigit(r)
	})
}

// clickHouseHostProblem returns "" when every address names a host, "no_usable_host" when none does, else
// "empty_host_element".
func clickHouseHostProblem(parsed *url.URL) string {
	usable, total := 0, 0
	for _, addr := range clickHouseAddrs(parsed) {
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
