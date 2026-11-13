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
	"os"
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

// IdPAllowEnv names the comma-separated CIDR list of non-public addresses an SSO OIDC call (discovery, token
// exchange, JWKS, userinfo) may still dial: a job-local IdP only; never set in prod. Unset or empty: none, and
// IdPTransport is GuardedTransport. One invalid or forbidden entry: none (fail closed).
const IdPAllowEnv = "SSO_EGRESS_ALLOW_CIDRS"

// IdPTransport is the transport of the SSO OIDC clients only. With IdPAllowEnv unset or empty it is GuardedTransport
// (the same dial check, the same function). With a valid list, a non-public address inside it is dialled when its
// class is not a never-allowed one and it is not a translation address; every other address is refused exactly as
// GuardedTransport refuses it. The list is read when the transport is built.
func IdPTransport() *http.Transport {
	transport := httpguard.Transport()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: idpDialControl(os.Getenv(IdPAllowEnv)),
		Resolver: dialResolver}
	transport.DialContext = dialer.DialContext
	return transport
}

type dialControl = func(network, address string, conn syscall.RawConn) error

// idpDialControl is refuseInternalAddress itself when value lists nothing usable.
func idpDialControl(value string) dialControl {
	allow := parseIdPAllow(value)
	if len(allow) == 0 {
		return refuseInternalAddress
	}
	return refuseUnlessIdPAllowed(allow)
}

// parseIdPAllow reads the list. A set, valid list writes one WARN with its entry count; an entry that is not a CIDR,
// or that holds an address no allow list may admit (netclass.AllowEntryForbidden), empties the whole list and writes
// one ERROR with the entry count. No line carries an entry.
func parseIdPAllow(value string) []netip.Prefix {
	var fields []string
	for _, field := range strings.Split(value, ",") {
		if field = strings.TrimSpace(field); field != "" {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return nil
	}
	prefixes := make([]netip.Prefix, 0, len(fields))
	for _, field := range fields {
		prefix, err := netip.ParsePrefix(field)
		if err != nil || netclass.AllowEntryForbidden(prefix) {
			slog.Error("externalurl: "+IdPAllowEnv+" has an invalid or forbidden entry; no non-public address is allowed",
				"entries", len(fields))
			return nil
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	slog.Warn("externalurl: "+IdPAllowEnv+" in force", "entries", len(prefixes))
	return prefixes
}

// refuseUnlessIdPAllowed admits a non-public address inside allow whose class is not never-allowed and that is not a
// translation address (one INFO with the class only); every other address goes to refuseInternalAddress.
func refuseUnlessIdPAllowed(allow []netip.Prefix) dialControl {
	return func(network, address string, conn syscall.RawConn) error {
		if host, _, err := net.SplitHostPort(address); err == nil {
			if addr, err := netip.ParseAddr(host); err == nil {
				addr = addr.Unmap().WithZone("")
				class := netclass.BlockedAddressClass(addr)
				if class != "" && !netclass.NeverAllowed(class) && len(netclass.TranslationTargets(addr)) == 0 && inPrefixes(addr, allow) {
					slog.Info("externalurl: outbound connection allowed by "+IdPAllowEnv, "class", class)
					return nil
				}
			}
		}
		return refuseInternalAddress(network, address, conn)
	}
}

func inPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}
