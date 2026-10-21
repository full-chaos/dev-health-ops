// Package externalurl is the SSRF guard the credential and team admin routes
// apply to a stored provider URL before any outbound call.
package externalurl

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/netclass"
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
		if netclass.PythonNotGlobal(addr) {
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

// GuardedTransport is the transport for an outbound call whose URL Validate
// approved. Validate resolves the host once and discards the answer, so a
// second resolution at connect time could land on an internal address
// (DNS rebinding); this transport checks the address each connection
// actually dials, with the same classification Validate applies, and refuses
// an internal one before any byte (a bearer token included) is written. It
// never uses an environment proxy: the address that is dialled must be the
// address that was checked.
func GuardedTransport() *http.Transport {
	transport := httpguard.Transport()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: refuseInternalAddress, Resolver: dialResolver}
	transport.DialContext = dialer.DialContext
	return transport
}

// dialResolver is the resolver the guarded dialer uses; nil is the system
// resolver. A test sets it to prove a name that resolved public at check time
// is refused when it resolves internal at dial time (DNS rebinding).
var dialResolver *net.Resolver

// errInternalDial is what a refused connection reports; it is Validate's own
// wording for a resolved internal address.
var errInternalDial = errors.New("Connection to private/internal networks is not allowed")

// refuseInternalAddress is net.Dialer.Control: it runs on the resolved
// address of every connection attempt, before connect. A refusal logs the
// blocked class only, never the host or the address (a customer-supplied URL
// can carry a name or an internal address worth keeping out of logs).
func refuseInternalAddress(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		logRefusedDial("invalid_address")
		return errInternalDial
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		logRefusedDial("invalid_address")
		return errInternalDial
	}
	if class := netclass.BlockedAddressClass(addr); class != "" {
		logRefusedDial(class)
		return errInternalDial
	}
	return nil
}

func logRefusedDial(class string) {
	slog.Warn("externalurl: outbound connection refused", "class", class)
}
