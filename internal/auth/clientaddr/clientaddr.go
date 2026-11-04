// Package clientaddr is the client of one auth-service request: its address
// and its user agent.
//
// The address is the socket peer, unless the peer is a configured trusted
// proxy (AUTH_SERVICE_TRUSTED_PROXIES; go-api's TRUSTED_PROXIES is read with
// the same parser and the same rule, internal/api/clientip): then it is the
// right-most untrusted X-Forwarded-For hop (Trust.ClientOf). An excluded
// address ("!<address>" entry) is never a proxy, even inside a listed network:
// the node pod range holds the node's own addresses, from which a host process
// or a hostNetwork pod reaches the service. The internal service port binds
// the address with an empty Trust, so it never reads a forwarded value.
// X-Forwarded-Proto and the other forwarded headers are never read.
// AUTH_CLIENT_ADDR_MODE (proof.go, Policy) can name the client from go-api's
// signed X-Forwarded-Client-Proof instead; mode cidr, the default, is the rule
// above.
//
// The user agent is the first User-Agent value, each byte decoded as one
// latin-1 code point, as go-api's userAgent() reads it
// (internal/api/session/tokens.go). It is kept whole: the API listener's
// request-head cap (httpapi.ServerOptions.MaxHeaderBytes, 64 KiB by default;
// a larger head is refused with 431) is the only bound.
package clientaddr

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// Env names the trusted-proxy list.
const Env = "AUTH_SERVICE_TRUSTED_PROXIES"

// Trust is the set of peers whose X-Forwarded-For is believed.
type Trust struct {
	addrs    map[netip.Addr]bool
	prefixes []netip.Prefix
	except   map[netip.Addr]bool
}

// ParseTrust reads a comma-separated list of IP addresses, networks and
// exclusions. A network must be a strict CIDR (no host bits), never a /0. An
// exclusion is "!" and one IP address, never a network; it must lie inside a
// listed network and must not also be listed as an address. No entry may be an
// IPv4-mapped form. An empty value trusts no peer. "*", a hostname, an empty
// entry, a non-strict CIDR or a bad exclusion is an error, so a typo cannot
// widen the trust.
func ParseTrust(value string) (Trust, error) { return ParseTrustFor(Env, value) }

// ParseTrustFor is ParseTrust for the variable name, which its errors name.
func ParseTrustFor(name, value string) (Trust, error) {
	t := Trust{addrs: map[netip.Addr]bool{}, except: map[netip.Addr]bool{}}
	if strings.TrimSpace(value) == "" {
		return t, nil
	}
	type exclusion struct {
		addr  netip.Addr
		entry int
	}
	var exclusions []exclusion
	for index, raw := range strings.Split(value, ",") {
		entry := strings.TrimSpace(raw)
		switch {
		case entry == "":
			return Trust{}, fmt.Errorf("%s entry %d is empty", name, index+1)
		case entry == "*":
			return Trust{}, fmt.Errorf("%s entry %d is \"*\": name each proxy address or network", name, index+1)
		case strings.HasPrefix(entry, "!"):
			addr, err := netip.ParseAddr(entry[1:])
			if err != nil || addr.Zone() != "" {
				return Trust{}, fmt.Errorf("%s entry %d is not an excluded IP address (\"!\" and one address, never a network)", name, index+1)
			}
			if addr.Is4In6() {
				return Trust{}, fmt.Errorf("%s entry %d is an IPv4-mapped exclusion: give it as IPv4", name, index+1)
			}
			exclusions = append(exclusions, exclusion{addr, index + 1})
		case strings.Contains(entry, "/"):
			prefix, err := netip.ParsePrefix(entry)
			if err != nil || prefix.Addr().Zone() != "" {
				return Trust{}, fmt.Errorf("%s entry %d is not a network", name, index+1)
			}
			if prefix.Masked() != prefix {
				return Trust{}, fmt.Errorf("%s entry %d has host bits set: give the network address", name, index+1)
			}
			if prefix.Addr().Is4In6() {
				return Trust{}, fmt.Errorf("%s entry %d is an IPv4-mapped network: give it as IPv4", name, index+1)
			}
			if prefix.Bits() == 0 {
				return Trust{}, fmt.Errorf("%s entry %d is a /0 network: it would trust every peer", name, index+1)
			}
			t.prefixes = append(t.prefixes, prefix)
		default:
			addr, err := netip.ParseAddr(entry)
			if err != nil || addr.Zone() != "" {
				return Trust{}, fmt.Errorf("%s entry %d is not an IP address", name, index+1)
			}
			if addr.Is4In6() {
				return Trust{}, fmt.Errorf("%s entry %d is an IPv4-mapped address: give it as IPv4", name, index+1)
			}
			t.addrs[addr] = true
		}
	}
	for _, x := range exclusions {
		if t.addrs[x.addr] {
			return Trust{}, fmt.Errorf("%s entry %d excludes an address the list also trusts", name, x.entry)
		}
		if !t.inPrefixes(x.addr) {
			return Trust{}, fmt.Errorf("%s entry %d excludes an address outside every listed network", name, x.entry)
		}
		t.except[x.addr] = true
	}
	return t, nil
}

// Len is the number of trusted entries; exclusions are not counted.
func (t Trust) Len() int { return len(t.addrs) + len(t.prefixes) }

// Exclusions is the number of excluded addresses.
func (t Trust) Exclusions() int { return len(t.except) }

// WidePrefixes is the number of trusted networks wider than a /28 (IPv4) or a
// /124 (IPv6): each trusts every address in it as a proxy.
func (t Trust) WidePrefixes() int {
	wide := 0
	for _, prefix := range t.prefixes {
		if prefix.Addr().BitLen()-prefix.Bits() > 4 {
			wide++
		}
	}
	return wide
}

// Contains reports whether addr (zone dropped, IPv4-mapped unmapped) is a
// trusted proxy: listed, or inside a listed network and not excluded.
func (t Trust) Contains(addr netip.Addr) bool { return t.trusts(addr.WithZone("").Unmap()) }

func (t Trust) inPrefixes(peer netip.Addr) bool {
	for _, prefix := range t.prefixes {
		if prefix.Contains(peer) {
			return true
		}
	}
	return false
}

func (t Trust) trusts(peer netip.Addr) bool {
	if t.except[peer] {
		return false
	}
	if t.addrs[peer] {
		return true
	}
	return t.inPrefixes(peer)
}

// Resolve is the client address of r: the zero Addr when the peer address
// does not parse. It is ClientOf over the request's socket peer and headers.
func (t Trust) Resolve(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return t.ClientOf(peer.WithZone("").Unmap(), r.Header.Values("X-Forwarded-For"), r.Header.Get("X-Real-IP"))
}

// ClientOf is the one rule that names the client, for go-api and auth-service alike:
//
//  1. An untrusted peer is the client; its forwarding headers are never read.
//  2. For a trusted peer the X-Forwarded-For lines are one chain, walked from the RIGHT. A trusted hop
//     is skipped as a proxy; the first untrusted hop is the client. Entries left of it were written by
//     that hop's sender and are never read.
//  3. A hop that is not an address (empty, a name, garbage) ends the walk: the peer is the client.
//  4. With no chain, or a chain of trusted hops only, a valid untrusted X-Real-IP is the client; else the peer.
//
// The caller lists a proxy only if it appends its peer to X-Forwarded-For or replaces the header with it.
// An empty Trust trusts nobody, so the answer is always the peer.
func (t Trust) ClientOf(peer netip.Addr, forwardedFor []string, realIP string) netip.Addr {
	peer = peer.WithZone("").Unmap()
	if !t.trusts(peer) {
		return peer
	}
	if joined := strings.Join(forwardedFor, ","); strings.TrimSpace(joined) != "" {
		hops := strings.Split(joined, ",")
		for i := len(hops) - 1; i >= 0; i-- {
			hop, ok := ParseHop(hops[i])
			if !ok {
				return peer
			}
			if !t.trusts(hop) {
				return hop
			}
		}
	}
	if real, ok := ParseHop(realIP); ok && !t.trusts(real) {
		return real
	}
	return peer
}

// ParseHop reads one address as a proxy header carries it: a bare IP, IP:port, [IPv6] or [IPv6]:port, spaces
// ignored. A zone is dropped and an IPv4-mapped IPv6 address is unmapped, so one client has one key.
func ParseHop(text string) (netip.Addr, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return netip.Addr{}, false
	}
	if addr, err := netip.ParseAddr(text); err == nil {
		return addr.Unmap().WithZone(""), true
	}
	if ap, err := netip.ParseAddrPort(text); err == nil {
		return ap.Addr().Unmap().WithZone(""), true
	}
	if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
		if addr, err := netip.ParseAddr(text[1 : len(text)-1]); err == nil {
			return addr.Unmap().WithZone(""), true
		}
	}
	return netip.Addr{}, false
}

// TrustsPeer reports whether the socket peer of r is a trusted proxy.
func (t Trust) TrustsPeer(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return t.trusts(peer.WithZone("").Unmap())
}

type key struct{}

type userAgentKey struct{}

// Middleware binds the resolved client address and the user agent to every
// request: PolicyMiddleware in mode cidr with no proof key.
func Middleware(t Trust) func(http.Handler) http.Handler {
	return PolicyMiddleware(Policy{Mode: ModeCIDR, Trust: t})
}

func latin1(raw string) string {
	var builder strings.Builder
	builder.Grow(len(raw))
	for index := 0; index < len(raw); index++ {
		builder.WriteRune(rune(raw[index]))
	}
	return builder.String()
}

// WithUserAgent binds agent to ctx.
func WithUserAgent(ctx context.Context, agent string) context.Context {
	return context.WithValue(ctx, userAgentKey{}, agent)
}

// UserAgent returns the user agent bound to ctx.
func UserAgent(ctx context.Context) (string, bool) {
	agent, ok := ctx.Value(userAgentKey{}).(string)
	return agent, ok
}

// With binds addr to ctx.
func With(ctx context.Context, addr netip.Addr) context.Context {
	return context.WithValue(ctx, key{}, addr)
}

// From returns the address bound to ctx.
func From(ctx context.Context) (netip.Addr, bool) {
	addr, ok := ctx.Value(key{}).(netip.Addr)
	return addr, ok && addr.IsValid()
}

// Text is the bound address as text for an inet column, or nil.
func Text(ctx context.Context) *string {
	addr, ok := From(ctx)
	if !ok {
		return nil
	}
	text := addr.String()
	return &text
}
