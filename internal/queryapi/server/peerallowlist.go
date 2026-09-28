package server

import (
	"context"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// D2953 (gwc-bigboy-ops-3, proven live in a throwaway compose project): the
// internal listener (CHAOS-6780, QUERY_API_INTERNAL_ADDR) binds every
// interface -- on compose, any service on ANY network the container belongs
// to reaches it and can send the token-free X-DH-Internal-* identity
// headers. This file adds a peer-address allowlist: a connection is accepted
// only when its remote IP is inside a configured CIDR list, checked at
// accept time, never from a header (X-Forwarded-For is never consulted --
// it is caller-supplied and this check exists precisely because a caller's
// claims are not trusted here).
//
// Unset = today's behaviour: every peer is accepted, and Start logs once,
// loudly, that this listener has no peer allowlist. This ADDS a layer; it
// removes none -- prod continues to rely on its NetworkPolicy as the
// primary boundary.

func cidrsPermit(nets []*net.IPNet, ip net.IP) bool {
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

var peerRefusedCounter = mustPeerRefusedCounter()

func mustPeerRefusedCounter() metric.Int64Counter {
	const name = "devhealth_query_api_internal_peer_refused_total"
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/server").Int64Counter(
		name,
		metric.WithDescription("Connections refused on query-api's internal listener because the peer address was outside the configured allowlist"),
	)
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// peerRefusalLogGate lets a refusal log line through at most once a second,
// the same shape internalidentity's own drop-log-gate uses -- the counter
// still counts every refusal, so a flood is never invisible, only quiet on
// the log.
var peerRefusalLogGate atomic.Int64

func logPeerRefusalRateLimited(remote string) {
	if last, now := peerRefusalLogGate.Load(), time.Now().UnixNano(); now-last >= int64(time.Second) && peerRefusalLogGate.CompareAndSwap(last, now) {
		log.Printf("query-api: refused a connection on the internal listener: peer address outside the configured allowlist, remote=%s", remote)
	}
}

// allowlistListener wraps a net.Listener so Accept only ever returns a
// connection whose remote address is inside allowed. A refused connection is
// closed immediately, counted, and logged at a bounded rate -- it never
// reaches http.Server.Serve, so no handler for it ever runs.
type allowlistListener struct {
	net.Listener
	allowed []*net.IPNet
}

func newAllowlistListener(inner net.Listener, allowed []*net.IPNet) net.Listener {
	if len(allowed) == 0 {
		return inner
	}
	return &allowlistListener{Listener: inner, allowed: allowed}
}

// peerIP parses a net.Conn.RemoteAddr().String() into the bare IP a CIDR
// match compares against, stripped of its port and (for a link-local IPv6
// peer) its zone identifier.
//
// A scoped address ("[fe80::1%eth0]:12345") carries a zone naming WHICH
// interface it arrived on -- net.ParseIP does not accept that suffix at all
// (returns nil for the whole string), so every such peer was refused
// outright before this fix, even one genuinely inside an allowed CIDR
// (codex r1, executed repro: a real fe80::/10 allowlist entry refused a
// real link-local peer at that exact address). The zone says nothing a CIDR
// match needs -- net.IPNet.Contains compares bytes, not scope -- so it is
// stripped before parsing, never consulted. Returns nil if remoteAddr does
// not parse as host:port or the host is not an IP at all.
func peerIP(remoteAddr string) net.IP {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil
	}
	if zone := strings.IndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	return net.ParseIP(host)
}

func (l *allowlistListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		remote := conn.RemoteAddr().String()
		if ip := peerIP(remote); ip != nil && cidrsPermit(l.allowed, ip) {
			return conn, nil
		}
		_ = conn.Close()
		peerRefusedCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("listener", "query-internal-http")))
		logPeerRefusalRateLimited(remote)
	}
}
