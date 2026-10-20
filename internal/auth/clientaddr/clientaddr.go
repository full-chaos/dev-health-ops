// Package clientaddr is the client address of one auth-service request.
//
// The address is the socket peer, unless the peer is a configured trusted
// proxy (AUTH_SERVICE_TRUSTED_PROXIES) that sent exactly one X-Forwarded-For
// value holding exactly one address: then it is that address. The go-api
// proxy sends one value it resolved itself (internal/apiservice/authproxy.go),
// so a chain, a second header or an unparsable value is not a proxy's answer
// and falls back to the peer. X-Forwarded-Proto and the other forwarded
// headers are never read.
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
}

// ParseTrust reads a comma-separated list of IP addresses and networks. A
// network must be a strict CIDR (no host bits). An empty value trusts no
// peer. "*", a hostname, an empty entry or a non-strict CIDR is an error, so
// a typo cannot widen the trust.
func ParseTrust(value string) (Trust, error) {
	t := Trust{addrs: map[netip.Addr]bool{}}
	if strings.TrimSpace(value) == "" {
		return t, nil
	}
	for index, raw := range strings.Split(value, ",") {
		entry := strings.TrimSpace(raw)
		switch {
		case entry == "":
			return Trust{}, fmt.Errorf("%s entry %d is empty", Env, index+1)
		case entry == "*":
			return Trust{}, fmt.Errorf("%s entry %d is \"*\": name each proxy address or network", Env, index+1)
		case strings.Contains(entry, "/"):
			prefix, err := netip.ParsePrefix(entry)
			if err != nil || prefix.Addr().Zone() != "" {
				return Trust{}, fmt.Errorf("%s entry %d is not a network", Env, index+1)
			}
			if prefix.Masked() != prefix {
				return Trust{}, fmt.Errorf("%s entry %d has host bits set: give the network address", Env, index+1)
			}
			if prefix.Addr().Is4In6() {
				return Trust{}, fmt.Errorf("%s entry %d is an IPv4-mapped network: give it as IPv4", Env, index+1)
			}
			t.prefixes = append(t.prefixes, prefix)
		default:
			addr, err := netip.ParseAddr(entry)
			if err != nil || addr.Zone() != "" {
				return Trust{}, fmt.Errorf("%s entry %d is not an IP address", Env, index+1)
			}
			t.addrs[addr.Unmap()] = true
		}
	}
	return t, nil
}

// Len is the number of trusted entries.
func (t Trust) Len() int { return len(t.addrs) + len(t.prefixes) }

func (t Trust) trusts(peer netip.Addr) bool {
	if t.addrs[peer] {
		return true
	}
	for _, prefix := range t.prefixes {
		if prefix.Contains(peer) {
			return true
		}
	}
	return false
}

// Resolve is the client address of r: the zero Addr when the peer address
// does not parse.
func (t Trust) Resolve(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	peer = peer.WithZone("").Unmap()
	if !t.trusts(peer) {
		return peer
	}
	values := r.Header.Values("X-Forwarded-For")
	if len(values) != 1 || strings.Contains(values[0], ",") {
		return peer
	}
	forwarded, err := netip.ParseAddr(strings.TrimSpace(values[0]))
	if err != nil || forwarded.Zone() != "" {
		return peer
	}
	return forwarded.Unmap()
}

type key struct{}

// Middleware binds the resolved client address to every request.
func Middleware(t Trust) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addr := t.Resolve(r)
			if !addr.IsValid() {
				next.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r.WithContext(With(r.Context(), addr)))
		})
	}
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
