// Package clientip is the one place the Go api derives "who is the client"
// from a request, for rate-limit keys and audit rows (CHAOS-7204).
//
// The rule is clientaddr.Trust.ClientOf, the same code auth-service binds its client address with. A bypass
// here is a rate-limit or audit-spoofing bug:
//
//  1. The client is the TCP peer, unless the peer is a trusted proxy. An
//     untrusted peer's forwarding headers are ignored entirely.
//  2. Trusted proxies come from TRUSTED_PROXIES, read on every call with the
//     auth-service parser (clientaddr.ParseTrustFor): comma-separated IPs, strict
//     CIDRs and "!<address>" exclusions. An excluded address is never a proxy,
//     even inside a listed network: the node pod range holds the node's own
//     addresses, from which a host process or a hostNetwork pod reaches go-api.
//     Unset or empty trusts nobody. A list the parser refuses trusts nobody, and
//     go-api refuses to start on it (Check), so no entry is ever dropped.
//  3. For a trusted peer, X-Forwarded-For (all header lines joined) is walked
//     from the RIGHT: each hop a trusted proxy appended is skipped, and the
//     first untrusted hop is the client. Entries left of that hop are
//     client-supplied and are never read, so a spoofed leftmost entry cannot
//     change the result whatever the ingress does with the header.
//  4. A malformed entry reached during the walk never wins: the walk stops and
//     the peer is used.
//  5. When the trusted peer sent no X-Forwarded-For, or every hop in it was
//     trusted, a valid untrusted X-Real-IP is the client; else the peer.
//
// Trust contract (r1 finding, CHAOS-7204): a proxy listed in TRUSTED_PROXIES must
// APPEND its peer to X-Forwarded-For or REPLACE the header with that peer, so the
// rightmost hop is written by the proxy, not the client. Prod ingress-nginx runs
// replace mode (use-forwarded-headers=true, compute-full-forwarded-for=false:
// upstream sees only the real client). A proxy that forwards a client-written
// header UNCHANGED does not meet the contract: no peer-based rule can tell its
// client-written last hop from a real one, so do not list such a proxy.
//
// Every address in TRUSTED_PROXIES is believed to write a correct chain, and a
// hop inside the list is skipped as "a proxy" whoever really sent it (r2 finding).
// So list proxy addresses only, as narrowly as the network allows: an untrusted
// client whose own address falls inside a listed CIDR appears to the walk as a
// proxy hop, and the entries left of it -- which it wrote -- then win. A whole
// pod CIDR therefore trusts every pod in it as a proxy.
//
// Addresses come back canonical (IPv4-mapped IPv6 unmapped, zone, port and
// brackets dropped) so one client cannot split across several bucket keys.
package clientip

import (
	"net"
	"net/http"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// Env names the trusted-proxy list.
const Env = "TRUSTED_PROXIES"

// FromRequest returns the client address per the package rule, or "" when the
// request carries no peer address at all.
func FromRequest(r *http.Request) string {
	peerText := peerHost(r.RemoteAddr)
	peer, peerOK := clientaddr.ParseHop(peerText)
	if !peerOK {
		// Not an IP (unix socket, test harness): nothing to trust, key on the
		// raw text so distinct peers stay distinct.
		return peerText
	}
	return trustedSet().ClientOf(peer, r.Header.Values("X-Forwarded-For"), r.Header.Get("X-Real-IP")).String()
}

func peerHost(remoteAddr string) string {
	if remoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}

// trustedSet is the parsed list; a list the parser refuses trusts nobody.
func trustedSet() clientaddr.Trust {
	trust, err := clientaddr.ParseTrustFor(Env, os.Getenv(Env))
	if err != nil {
		return clientaddr.Trust{}
	}
	return trust
}

// Check reads TRUSTED_PROXIES as FromRequest does: the number of trusted entries
// and of exclusions, or the parser's error, which names the entry by its index.
func Check() (entries, exclusions int, err error) {
	trust, err := clientaddr.ParseTrustFor(Env, os.Getenv(Env))
	if err != nil {
		return 0, 0, err
	}
	return trust.Len(), trust.Exclusions(), nil
}

// WidePrefixes counts the TRUSTED_PROXIES networks wider than a /28 (IPv4) or a
// /124 (IPv6): each trusts every address in it as a proxy.
func WidePrefixes() int { return trustedSet().WidePrefixes() }

// NetworkFencedMarker names the variable the chart sets to "true" only where the go-api request
// ports are fenced to the ingress controller pods by a NetworkPolicy.
const NetworkFencedMarker = "TRUSTED_PROXIES_NETWORK_FENCED"

// NetworkFenced reports whether the chart declared the request ports fenced: a wide trust list is
// then reachable only by the proxies the policy admits.
func NetworkFenced() bool {
	return os.Getenv(NetworkFencedMarker) == "true"
}
