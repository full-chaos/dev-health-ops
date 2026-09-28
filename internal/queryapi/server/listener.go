package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
)

// Listener is one HTTP listener of the query plane as a lifecycle component: the
// address is bound in Start (so a taken port fails the start, not a goroutine), it
// serves until Shutdown, and a failure after the bind is reported on Errors so the
// shell stops the process instead of leaving it up with no listener.
type Listener struct {
	name   string
	server *http.Server
	errors chan error
	// allowedPeers, when non-empty, restricts Start's listener to accepting
	// only a connection whose remote address falls inside one of these
	// CIDRs (D2953) -- checked at accept time, before any handler runs.
	// Empty (the common case: the public listener, or an internal listener
	// with no configured allowlist) accepts every peer, exactly as before
	// this field existed.
	allowedPeers []*net.IPNet

	mu       sync.RWMutex
	listener net.Listener
}

// Name is the component's name in the shell's lifecycle logs.
func (l *Listener) Name() string { return l.name }

// Start binds the address and serves in the background.
func (l *Listener) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.listener != nil {
		return fmt.Errorf("%s is already started", l.name)
	}
	listener, err := net.Listen("tcp", l.server.Addr)
	if err != nil {
		return fmt.Errorf("listen for %s: %w", l.name, err)
	}
	listener = newAllowlistListener(listener, l.allowedPeers)
	l.listener = listener
	l.server.BaseContext = func(net.Listener) context.Context { return ctx }
	log.Printf("query-api: %s listening on %s", l.name, listener.Addr())
	go func() {
		if err := l.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case l.errors <- fmt.Errorf("%s: %w", l.name, err):
			default:
			}
		}
	}()
	return nil
}

// Shutdown stops accepting and lets in-flight requests finish within ctx.
func (l *Listener) Shutdown(ctx context.Context) error {
	l.mu.RLock()
	started := l.listener != nil
	l.mu.RUnlock()
	if !started {
		return nil
	}
	if err := l.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown %s: %w", l.name, err)
	}
	return nil
}

// Errors reports a serve failure after a successful bind.
func (l *Listener) Errors() <-chan error { return l.errors }

// Address is the bound address once started ("" before), with port 0 resolved.
func (l *Listener) Address() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.listener == nil {
		return ""
	}
	return l.listener.Addr().String()
}

// Listeners builds the public listener over the plane and, when internalAddr is
// set, the internal one (CHAOS-6780): the public one deletes the X-DH-Internal-*
// identity headers before any handler sees them, the internal one, on a port no
// Ingress routes to, alone honours them. internal is nil when internalAddr is empty.
//
// extra, when non-nil, answers the paths it declares (the one-release operator
// compatibility routes, OperatorCompat) and everything else goes to the plane's routes.
//
// internalAllowedPeers (D2953), when non-empty, restricts the internal listener to
// accepting a connection only from a peer address inside one of these CIDRs -- see
// peerallowlist.go. Empty means no restriction beyond whatever network segmentation
// already exists (a NetworkPolicy in prod), and Listeners logs that choice once,
// loudly, rather than silently: an internal listener with an open bind on every
// interface and no peer check is the exact gap D2953 was raised for. Ignored when
// internalAddr is empty (there is no internal listener to restrict).
func Listeners(publicAddr, internalAddr string, plane *Plane, extra http.Handler, internalAllowedPeers []*net.IPNet) (public, internal *Listener) {
	base := plane.Handler
	if extra != nil {
		mux := http.NewServeMux()
		mux.Handle("/healthz", extra)
		mux.Handle("/readyz", extra)
		mux.Handle("/metrics", extra)
		mux.Handle("/", plane.Handler)
		base = mux
	}
	publicServer, internalServer := newListenerServers(publicAddr, internalAddr, base)
	public = &Listener{name: "query-http", server: publicServer, errors: make(chan error, 1)}
	if internalServer != nil {
		if len(internalAllowedPeers) == 0 {
			log.Print("query-api: the internal listener has no peer allowlist (QUERY_API_INTERNAL_ALLOWED_CIDRS unset) -- every peer that can reach the port is accepted; prod relies on its NetworkPolicy for this boundary")
		}
		internal = &Listener{name: "query-internal-http", server: internalServer, errors: make(chan error, 1), allowedPeers: internalAllowedPeers}
	}
	return public, internal
}

// Closer is the lifecycle component that releases the routes' dependencies. It is
// started before the listeners, so the reverse shutdown order closes them only
// after the listeners stopped serving.
type Closer struct{ Plane *Plane }

func (Closer) Name() string                { return "query-routes" }
func (Closer) Start(context.Context) error { return nil }
func (c Closer) Shutdown(context.Context) error {
	if c.Plane != nil && c.Plane.Close != nil {
		c.Plane.Close()
	}
	return nil
}
