// Package egressguard is the dial-time SSRF guard for calls to a stored provider URL (a self-hosted GitHub, GitLab or
// Jira base URL, a Linear or PagerDuty endpoint): every connection, redirect hops included, goes only to a public
// address or one inside PROVIDER_EGRESS_ALLOW_CIDRS, and never through a proxy. It uses the address classes of
// externalurl (the guard the credential routes apply before they save a URL) and is built on httpguard's transport.
package egressguard

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/netclass"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// AllowEnv names the comma-separated CIDR list of non-public addresses a provider call may still dial (a
// self-hosted GitLab on a private network, a local fake). Unset or empty: none. One invalid entry: none (fail closed).
const AllowEnv = "PROVIDER_EGRESS_ALLOW_CIDRS"

// ErrRefused is what every refused provider dial matches with errors.Is.
var ErrRefused = errors.New("outbound connection refused: the address is not public")

// RefusedError names the class of the refused address; it never carries the host, the address or the URL.
type RefusedError struct{ Class string }

func (e *RefusedError) Error() string {
	return "outbound connection refused: the address is not public (" + e.Class + ")"
}

func (e *RefusedError) Is(target error) bool { return target == ErrRefused }

// EgressRefusedClass lets a package that cannot import this one (platform/logging) name the refusal by its type.
func (e *RefusedError) EgressRefusedClass() string { return e.Class }

// neverAllowed are the classes no allow list admits: no provider lives there, and the metadata address hands out the
// node's cloud credentials.
var neverAllowed = map[string]bool{"metadata": true, "link_local": true, "multicast": true, "unspecified": true, "invalid_address": true}

var (
	allowFromEnv = sync.OnceValue(func() []netip.Prefix { return parseAllow(os.Getenv(AllowEnv)) })
	allowForTest atomic.Pointer[[]netip.Prefix]
)

func parseAllow(value string) []netip.Prefix {
	var prefixes []netip.Prefix
	for _, field := range strings.Split(value, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(field)
		if err != nil {
			synclog.Default().Error(context.Background(), synclog.MsgEgressAllowListInvalid, synclog.Count(synclog.KeyEgressEntries, len(strings.Split(value, ","))))
			return nil
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixes
}

func allowList() []netip.Prefix {
	if override := allowForTest.Load(); override != nil {
		return *override
	}
	return allowFromEnv()
}

// SetLookupForTest replaces the name lookup of the guarded dialer until restore runs; tests only (the same census
// keeps production code from calling it).
func SetLookupForTest(fn func(ctx context.Context, host string) ([]netip.Addr, error)) (restore func()) {
	previous := lookup
	lookup = fn
	return func() { lookup = previous }
}

// SetAllowForTest replaces the allow list for this process until restore runs; tests only (production code never
// calls it: TestNoProductionCallOfTheTestSeams).
func SetAllowForTest(prefixes ...netip.Prefix) (restore func()) {
	previous := allowForTest.Swap(&prefixes)
	return func() { allowForTest.Store(previous) }
}

// check is nil for a public address or an allowed one, else the classed refusal (one WARN with the class only).
func check(addr netip.Addr) error {
	addr = addr.Unmap().WithZone("")
	// A translation address (NAT64, 6to4, Teredo) is decided on the IPv4 addresses it reaches, each checked and allowed
	// as itself: an allow list for the IPv6 translation range never admits a non-public target, and a public target is
	// public. A metadata form is refused first.
	if targets := netclass.TranslationTargets(addr); len(targets) > 0 {
		if class := netclass.BlockedAddressClass(addr); class == "metadata" {
			logRefused(class)
			return &RefusedError{Class: class}
		}
		for _, target := range targets {
			if err := check(target); err != nil {
				return err
			}
		}
		return nil
	}
	class := netclass.BlockedAddressClass(addr)
	if class == "" {
		return nil
	}
	if !neverAllowed[class] && inAny(addr, allowList()) {
		return nil
	}
	logRefused(class)
	return &RefusedError{Class: class}
}

// lookup resolves a dial host; an IP literal is itself. A test replaces it to map a name to an address.
var lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{addr}, nil
	}
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

type dialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// guardedDial resolves the host ONCE, refuses the whole dial when any answer is not public (and not allowed), then dials
// the checked address literal through inner: no second lookup, so a name cannot rebind between check and connect, and
// an inner dialer's own checks still run on that literal. TLS keeps the request's host name (net/http takes SNI and Host
// from the request, not from the dialled address).
func guardedDial(inner dialFunc) dialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			logRefused("invalid_address")
			return nil, &RefusedError{Class: "invalid_address"}
		}
		addrs, err := lookup(ctx, host)
		if err != nil {
			return nil, err
		}
		var candidates []netip.Addr
		for _, addr := range addrs {
			if err := check(addr); err != nil {
				return nil, err
			}
			addr = addr.Unmap()
			if (network == "tcp4" && !addr.Is4()) || (network == "tcp6" && !addr.Is6()) {
				continue
			}
			candidates = append(candidates, addr)
		}
		if len(candidates) == 0 {
			return nil, &net.DNSError{Err: "no address for the network", Name: host, IsNotFound: true}
		}
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return dialParallel(ctx, inner, network, port, candidates)
	}
}

// dialTimeout and fallbackDelay mirror the net.Dialer the guard builds when the source transport has no DialContext
// (and http.DefaultTransport): a 30 s budget for the whole dial, and the stdlib default FallbackDelay of 300 ms
// (net/dial.go fallbackDelay, GOROOT go1.27). A custom source DialContext is an opaque func: its own Dialer fields are
// not visible here, so the race always runs for it.
const (
	dialTimeout   = 30 * time.Second
	fallbackDelay = 300 * time.Millisecond
)

var errDialBudget = errors.New("dial: out of time")

// partition splits the checked addresses by the family of the first one (net/ipsock.go partition): that family is the
// primary, the other the fallback.
func partition(addrs []netip.Addr) (primaries, fallbacks []netip.Addr) {
	primaryIs4 := addrs[0].Is4()
	for _, addr := range addrs {
		if addr.Is4() == primaryIs4 {
			primaries = append(primaries, addr)
		} else {
			fallbacks = append(fallbacks, addr)
		}
	}
	return primaries, fallbacks
}

// partialDeadline is net/dial.go partialDeadline: with several addresses pending, each gets an equal share of the time
// left, never below 2 s (or what remains), so one dead address cannot eat the whole budget.
func partialDeadline(now, deadline time.Time, addrsRemaining int) (time.Time, error) {
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return time.Time{}, errDialBudget
	}
	timeout := remaining / time.Duration(addrsRemaining)
	const saneMinimum = 2 * time.Second
	if timeout < saneMinimum {
		if remaining < saneMinimum {
			timeout = remaining
		} else {
			timeout = saneMinimum
		}
	}
	return now.Add(timeout), nil
}

// dialSerial is net/dial.go dialSerial over checked literals: the first connection wins, the first error is returned.
func dialSerial(ctx context.Context, inner dialFunc, network, port string, addrs []netip.Addr) (net.Conn, error) {
	var firstErr error
	for i, addr := range addrs {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		dialCtx := ctx
		if deadline, ok := ctx.Deadline(); ok {
			share, err := partialDeadline(time.Now(), deadline, len(addrs)-i)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
			if share.Before(deadline) {
				var cancel context.CancelFunc
				dialCtx, cancel = context.WithDeadline(ctx, share)
				defer cancel()
			}
		}
		conn, err := inner(dialCtx, network, net.JoinHostPort(addr.String(), port))
		if err == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
}

// dialParallel is net/dial.go dialParallel: the primary family dials at once, the other family starts after
// fallbackDelay (or at once when the primary fails first); the first connection wins and the other is cancelled and
// closed. Only addresses that passed check ever reach inner.
func dialParallel(ctx context.Context, inner dialFunc, network, port string, candidates []netip.Addr) (net.Conn, error) {
	primaries, fallbacks := partition(candidates)
	if len(fallbacks) == 0 {
		return dialSerial(ctx, inner, network, port, primaries)
	}
	returned := make(chan struct{})
	defer close(returned)
	type result struct {
		conn    net.Conn
		err     error
		primary bool
		done    bool
	}
	results := make(chan result)
	race := func(ctx context.Context, primary bool) {
		addrs := primaries
		if !primary {
			addrs = fallbacks
		}
		conn, err := dialSerial(ctx, inner, network, port, addrs)
		select {
		case results <- result{conn: conn, err: err, primary: primary, done: true}:
		case <-returned:
			if conn != nil {
				_ = conn.Close()
			}
		}
	}
	var primary, fallback result
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go race(primaryCtx, true)
	timer := time.NewTimer(fallbackDelay)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go race(fallbackCtx, false)
		case res := <-results:
			if res.err == nil {
				return res.conn, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, primary.err
			}
			if res.primary && timer.Stop() {
				timer.Reset(0)
			}
		}
	}
}

// refusing answers every request with the refusal: the transport it replaces dials in a way the guard cannot see (its
// own Dial, DialTLS or DialTLSContext: unsupported_dialer) or is a RoundTripper the guard cannot reach into
// (unguarded_transport).
type refusing struct{ class string }

func (r refusing) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		_ = request.Body.Close()
	}
	logRefused(r.class)
	return nil, &RefusedError{Class: r.class}
}

// Offline is a RoundTripper that opens no network connection (it answers from memory or a recording): the guard has
// nothing to check and passes it through. Only test support may implement it (TestNoProductionCallOfTheTestSeams).
type Offline interface {
	http.RoundTripper
	EgressOffline()
}

var unguardedForTest atomic.Bool

// SetUnguardedTransportsForTest lets Wrap pass every RoundTripper it cannot reach into (a test double) through as it
// is until restore runs; tests only. Production refuses such a transport (fail closed).
func SetUnguardedTransportsForTest(allow bool) (restore func()) {
	previous := unguardedForTest.Swap(allow)
	return func() { unguardedForTest.Store(previous) }
}

func guardedClone(source *http.Transport) http.RoundTripper {
	if source.Dial != nil || source.DialTLS != nil || source.DialTLSContext != nil {
		return refusing{class: "unsupported_dialer"}
	}
	inner := dialFunc(source.DialContext)
	if inner == nil {
		inner = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	clone := source.Clone()
	clone.Proxy = nil
	clone.OnProxyConnectResponse = nil
	clone.DialContext = guardedDial(inner)
	return clone
}

// clones keeps one guarded clone per source transport (one connection pool, however often a caller asks), keyed
// weakly so a source transport that is collected takes its clone with it. A clone never refers to its source.
var clones sync.Map // weak.Pointer[http.Transport] -> http.RoundTripper

// guardedClones holds every clone cloneOf made, so guarding one again returns it unchanged.
var guardedClones sync.Map // weak.Pointer[http.Transport] -> struct{}

func cloneOf(source *http.Transport) http.RoundTripper {
	key := weak.Make(source)
	if _, guarded := guardedClones.Load(key); guarded {
		return source
	}
	if cached, ok := clones.Load(key); ok {
		return cached.(http.RoundTripper)
	}
	cached, loaded := clones.LoadOrStore(key, guardedClone(source))
	if !loaded {
		runtime.AddCleanup(source, func(key weak.Pointer[http.Transport]) { clones.Delete(key) }, key)
		if clone, ok := cached.(*http.Transport); ok {
			cloneKey := weak.Make(clone)
			guardedClones.Store(cloneKey, struct{}{})
			runtime.AddCleanup(clone, func(key weak.Pointer[http.Transport]) { guardedClones.Delete(key) }, cloneKey)
		}
	}
	return cached.(http.RoundTripper)
}

// Wrap is rt with the provider egress guard at dial time: every connection, redirect hops included, goes
// only to an address that is public or inside AllowEnv, and never through a proxy. nil is
// httpguard.SharedTransport() guarded; an *http.Transport is its guarded clone (Proxy nil); an
// httpguard.TransportWrapper is rebuilt around its guarded inner transport; an Offline transport is returned as it is;
// any other RoundTripper is replaced by one that refuses every request (unguarded_transport): the guard cannot see
// how it connects, so it fails closed.
func Wrap(rt http.RoundTripper) http.RoundTripper {
	switch t := rt.(type) {
	case nil:
		return cloneOf(httpguard.SharedTransport())
	case *http.Transport:
		if t == nil {
			return cloneOf(httpguard.SharedTransport())
		}
		return cloneOf(t)
	case httpguard.TransportWrapper:
		rebuilt := t.RewrapTransport(Wrap(t.UnwrapTransport()))
		if isNilTransport(rebuilt) {
			// An http.Client would fall back to http.DefaultTransport (unguarded, environment proxy): fail closed.
			return refusing{class: "unguarded_transport"}
		}
		return rebuilt
	case Offline:
		return rt
	default:
		if unguardedForTest.Load() {
			return rt
		}
		return refusing{class: "unguarded_transport"}
	}
}

func inAny(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// logRefused is the one WARN of a refused dial: the class only, never the host, the address or the URL. It goes through
// synclog, the logger whose values cannot carry free text.
func logRefused(class string) {
	synclog.Default().Warn(context.Background(), synclog.MsgEgressRefused, synclog.Text(synclog.KeyEgressClass, synclog.ParseLabel(class)))
}

// isNilTransport is a nil RoundTripper, also a typed nil pointer inside the interface.
func isNilTransport(rt http.RoundTripper) bool {
	if rt == nil {
		return true
	}
	value := reflect.ValueOf(rt)
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Interface, reflect.Chan:
		return value.IsNil()
	}
	return false
}
