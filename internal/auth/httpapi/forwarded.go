package httpapi

import (
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ForwardedTrust is uvicorn's ProxyHeadersMiddleware trust list
// (FORWARDED_ALLOW_IPS, default "127.0.0.1"): "*" trusts every peer; other
// comma-separated entries are IP addresses, networks (a strict CIDR: no
// host bits), or literals compared as text.
type ForwardedTrust struct {
	all      bool
	addrs    map[netip.Addr]bool
	networks []netip.Prefix
	literals map[string]bool
}

// ParseForwardedTrust parses a FORWARDED_ALLOW_IPS value as uvicorn does.
func ParseForwardedTrust(value string) ForwardedTrust {
	trust := ForwardedTrust{addrs: map[netip.Addr]bool{}, literals: map[string]bool{}}
	if value == "*" {
		trust.all = true
		return trust
	}
	for _, raw := range strings.Split(value, ",") {
		host := strings.TrimFunc(raw, pythonparity.IsSpace)
		if strings.Contains(host, "/") {
			if prefix, err := netip.ParsePrefix(host); err == nil && prefix.Masked() == prefix {
				trust.networks = append(trust.networks, prefix)
				continue
			}
			trust.literals[host] = true
			continue
		}
		if addr, err := netip.ParseAddr(host); err == nil {
			trust.addrs[addr] = true
			continue
		}
		trust.literals[host] = true
	}
	return trust
}

// trusts is _TrustedHosts.__contains__ for a peer host.
func (t ForwardedTrust) trusts(host string) bool {
	if t.all {
		return true
	}
	if host == "" {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return t.literals[host]
	}
	if t.addrs[addr] {
		return true
	}
	for _, network := range t.networks {
		if network.Contains(addr) {
			return true
		}
	}
	return false
}

// scheme is the request scheme as the ASGI scope carries it: "https" for a
// TLS connection, else "http"; when the peer is trusted, a last
// X-Forwarded-Proto value of http, https, ws or wss (latin-1, stripped)
// replaces it, as uvicorn's ProxyHeadersMiddleware does for an http scope.
func (t ForwardedTrust) scheme(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if !t.trusts(host) {
		return scheme
	}
	values := r.Header.Values("X-Forwarded-Proto")
	if len(values) == 0 {
		return scheme
	}
	latin1 := make([]rune, 0, len(values[len(values)-1]))
	for _, b := range []byte(values[len(values)-1]) {
		latin1 = append(latin1, rune(b))
	}
	switch proto := strings.TrimFunc(string(latin1), pythonparity.IsSpace); proto {
	case "http", "https", "ws", "wss":
		return proto
	}
	return scheme
}
