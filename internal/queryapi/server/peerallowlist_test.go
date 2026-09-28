package server

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
)

// D2953: unset (nil) must be a no-op wrapper -- today's behaviour, byte for
// byte, not merely "behaves the same" by coincidence.
func TestNewAllowlistListenerIsANoOpWhenUnset(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	if got := newAllowlistListener(inner, nil); got != inner {
		t.Fatalf("newAllowlistListener with an empty list returned a wrapper, not the original listener")
	}
}

// allowlistPlane builds a real Plane and starts an internal listener with the
// given allowlist, and returns the internal listener's address plus a hit
// counter the plane's one route increments -- so a test can assert the
// handler ran (or, critically, never ran).
func allowlistListenerAddr(t *testing.T, allowed []*net.IPNet) (addr string, hits *atomic.Int64, shutdown func()) {
	t.Helper()
	plane := listenerPlane(t)
	hits = &atomic.Int64{}
	// Wrap the plane's handler so every request, however it is dispatched,
	// is observed -- proving "the handler never ran" needs a counter
	// independent of the response the client sees.
	countingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		plane.Handler.ServeHTTP(w, r)
	})
	countingPlane := &Plane{Handler: countingHandler, Close: plane.Close}
	_, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", countingPlane, nil, allowed)
	if internal == nil {
		t.Fatal("no internal listener built")
	}
	ctx := context.Background()
	if err := internal.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return internal.Address(), hits, func() { _ = internal.Shutdown(ctx) }
}

// D2953: a peer inside the allowlist is served normally.
func TestPeerInsideTheAllowlistIsServed(t *testing.T) {
	addr, hits, shutdown := allowlistListenerAddr(t, mustCIDRs(t, "127.0.0.1/32"))
	defer shutdown()
	resp, err := http.Get("http://" + addr + "/api/v1/not-a-route")
	if err != nil {
		t.Fatalf("a peer inside the allowlist must be served, got: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want the route's own 404 (the request reached the handler)", resp.StatusCode)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want exactly 1 -- the handler must have run", hits.Load())
	}
}

// D2953: a peer outside the allowlist is refused BEFORE any handler runs --
// the connection is closed at accept time, never reaching http.Server.Serve.
func TestPeerOutsideTheAllowlistIsRefusedBeforeAnyHandlerRuns(t *testing.T) {
	// Deliberately excludes 127.0.0.1, which is what this test process
	// always dials from.
	addr, hits, shutdown := allowlistListenerAddr(t, mustCIDRs(t, "10.0.0.0/8"))
	defer shutdown()
	resp, err := http.Get("http://" + addr + "/api/v1/not-a-route")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("a peer outside the allowlist got a real HTTP response (status %d), want the connection refused", resp.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatalf("hits = %d, want 0 -- the handler must never run for a refused peer", hits.Load())
	}
}

// Codex r1 (executed repro against a real dho query-api process bound to a
// real link-local interface): QUERY_API_INTERNAL_ALLOWED_CIDRS=fe80::/10
// refused a real peer connecting from fe80::17ff:fe00:3876%enp0s10 -- a
// scoped address genuinely inside that CIDR. net.ParseIP rejected the
// zone-qualified string outright, so peerIP returned nil and cidrsPermit
// was never even reached, exactly as if the peer had refused for being
// outside the allowlist.
func TestPeerIPStripsTheZoneFromALinkLocalIPv6Address(t *testing.T) {
	ip := peerIP("[fe80::17ff:fe00:3876%enp0s10]:59218")
	if ip == nil {
		t.Fatal("peerIP returned nil for a scoped IPv6 address -- this is the P1 codex r1 found")
	}
	nets := mustCIDRs(t, "fe80::/10")
	if !cidrsPermit(nets, ip) {
		t.Fatalf("parsed IP %v is not permitted by fe80::/10, want it to be -- the zone must not survive into the parsed value", ip)
	}
	if !ip.Equal(net.ParseIP("fe80::17ff:fe00:3876")) {
		t.Fatalf("peerIP = %v, want the zone-stripped address fe80::17ff:fe00:3876", ip)
	}
}

// A scoped address OUTSIDE the allowlist must still be refused -- stripping
// the zone must not turn the check into "any link-local address passes".
func TestPeerIPZoneStrippingStillRefusesAnAddressOutsideTheAllowlist(t *testing.T) {
	ip := peerIP("[fe80::dead:beef%enp0s10]:1234")
	if ip == nil {
		t.Fatal("peerIP returned nil, want a parsed address")
	}
	nets := mustCIDRs(t, "10.0.0.0/8")
	if cidrsPermit(nets, ip) {
		t.Fatal("a link-local address was permitted by an unrelated CIDR -- zone stripping must not widen the match")
	}
}

// Ordinary (non-scoped) addresses, IPv4 and IPv6, are unaffected.
func TestPeerIPHandlesOrdinaryAddressesUnchanged(t *testing.T) {
	if ip := peerIP("127.0.0.1:80"); ip == nil || !ip.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("peerIP(127.0.0.1:80) = %v", ip)
	}
	if ip := peerIP("[::1]:80"); ip == nil || !ip.Equal(net.ParseIP("::1")) {
		t.Fatalf("peerIP([::1]:80) = %v", ip)
	}
	if ip := peerIP("not-a-host-port"); ip != nil {
		t.Fatalf("peerIP(malformed) = %v, want nil", ip)
	}
}

func mustCIDRs(t *testing.T, entries ...string) []*net.IPNet {
	t.Helper()
	nets := make([]*net.IPNet, 0, len(entries))
	for _, e := range entries {
		_, n, err := net.ParseCIDR(e)
		if err != nil {
			t.Fatalf("bad test CIDR %q: %v", e, err)
		}
		nets = append(nets, n)
	}
	return nets
}
