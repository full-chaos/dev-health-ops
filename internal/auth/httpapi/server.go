package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Route is one explicitly registered method-and-path endpoint.
//
// Method and Pattern are separate fields rather than one "GET /v1/x" string so
// that a route cannot be registered without a method: ACP-ADR-01 §3 requires
// explicit method-and-path registration, and a single string makes the method
// optional by construction.
type Route struct {
	// Method is an uppercase HTTP method.
	Method string
	// Pattern is a rooted path pattern accepted by http.ServeMux.
	Pattern string
	// Handler serves the route. It closes over domain interfaces; this
	// package never sees domain types.
	Handler http.Handler
	// RateLimitPerSecond and RateLimitBurst override the server defaults for
	// this route. Zero means "use the server default", which is what makes
	// the limits route-SPECIFIC rather than one global bucket.
	RateLimitPerSecond float64
	RateLimitBurst     int
	// MaxBodyBytes overrides the server default body bound for this route.
	// Zero means the server default.
	MaxBodyBytes int64
}

// ServerOptions configures the API server.
type ServerOptions struct {
	Address        string
	Logger         *slog.Logger
	Routes         []Route
	RequestTimeout time.Duration
	MaxBodyBytes   int64
	RateLimit      float64
	RateLimitBurst int
	// Now is injectable so a test can drive the rate limiter's clock. Nil
	// means time.Now.
	Now func() time.Time
}

// Server is the auth API listener. It is a lifecycle.Component so the runtime,
// not this package and not a package-level variable, owns its shutdown.
type Server struct {
	logger *slog.Logger
	server *http.Server
	errors chan error

	mu       sync.RWMutex
	listener net.Listener
}

// NewServer validates the route set and builds the handler. It performs no
// I/O; nothing binds until Start.
func NewServer(options ServerOptions) (*Server, error) {
	if options.Address == "" {
		return nil, fmt.Errorf("api server address is required")
	}
	if options.RequestTimeout <= 0 {
		return nil, fmt.Errorf("api request timeout must be positive")
	}
	if options.MaxBodyBytes <= 0 {
		return nil, fmt.Errorf("api max body bytes must be positive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(discard{}, nil))
	}

	handler, err := buildHandler(options, logger)
	if err != nil {
		return nil, err
	}

	server := &Server{logger: logger, errors: make(chan error, 1)}
	server.server = &http.Server{
		Addr:    options.Address,
		Handler: handler,
		// ReadHeaderTimeout bounds a slow-header (Slowloris) client.
		ReadHeaderTimeout: 5 * time.Second,
		// ReadTimeout bounds a slow body.
		ReadTimeout: 15 * time.Second,
		// WriteTimeout is the HARD stop for a handler that ignores its
		// context deadline: see Deadline's doc comment for why this, and not
		// http.TimeoutHandler, is the backstop. It must exceed
		// RequestTimeout, or the connection would be torn down before a
		// well-behaved handler could render its own deadline response.
		WriteTimeout: options.RequestTimeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
		// MaxHeaderBytes bounds header memory independently of the body
		// bound, which MaxBody cannot see.
		MaxHeaderBytes: 1 << 16,
		ErrorLog:       slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	return server, nil
}

// buildHandler wires the mux and the middleware stack.
//
// Registration order and ServeMux precedence together are what make the error
// envelopes exact. Go's ServeMux prefers the pattern matching the strictest
// subset of requests, and "GET /v1/x" is a strict subset of "/v1/x", which is
// a strict subset of "/". So for every route path this registers BOTH the
// method-specific pattern and a path-only fallback carrying a 405 with an
// Allow header, plus one "/" catch-all carrying a 404. A correct request wins
// on the method pattern, a wrong method on a known path falls to that path's
// 405, and an unknown path falls to the catch-all -- all three rendered by
// this package's envelope rather than by net/http's plain-text defaults.
func buildHandler(options ServerOptions, logger *slog.Logger) (http.Handler, error) {
	mux := http.NewServeMux()

	methodsByPattern := make(map[string][]string)
	seen := make(map[string]struct{}, len(options.Routes))
	for _, route := range options.Routes {
		if route.Handler == nil {
			return nil, fmt.Errorf("route %s %s has no handler", route.Method, route.Pattern)
		}
		if route.Method == "" || route.Method != strings.ToUpper(route.Method) {
			return nil, fmt.Errorf("route method %q must be a non-empty uppercase method", route.Method)
		}
		if !strings.HasPrefix(route.Pattern, "/") {
			return nil, fmt.Errorf("route pattern %q must be rooted", route.Pattern)
		}
		key := route.Method + " " + route.Pattern
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("route %s is registered twice", key)
		}
		seen[key] = struct{}{}
		methodsByPattern[route.Pattern] = append(methodsByPattern[route.Pattern], route.Method)

		mux.Handle(key, routeChain(route, options, logger))
	}

	for pattern, methods := range methodsByPattern {
		sort.Strings(methods)
		allow := strings.Join(methods, ", ")
		mux.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allow)
			WriteError(w, r, CodeMethodNotAllowed)
		}))
	}

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, CodeNotFound)
	}))

	// RequestID is outermost so every response -- including the 404 and 405
	// envelopes above, which never reach a route chain -- carries a
	// correlation id. The outer Recover covers the mux itself and those two
	// handlers; a route's own Recover (inside routeChain) catches first and
	// logs the real pattern.
	return RequestID(Recover(logger, "<unrouted>")(mux)), nil
}

// routeChain wraps one route's handler. Order is deliberate: rate limiting is
// the cheapest rejection and runs first, then the body bound (which can reject
// on Content-Length without reading anything), then the deadline, then the
// route's own panic recovery closest to the handler so the log line names the
// real pattern.
func routeChain(route Route, options ServerOptions, logger *slog.Logger) http.Handler {
	perSecond := route.RateLimitPerSecond
	if perSecond <= 0 {
		perSecond = options.RateLimit
	}
	burst := route.RateLimitBurst
	if burst <= 0 {
		burst = options.RateLimitBurst
	}
	maxBody := route.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = options.MaxBodyBytes
	}

	handler := Recover(logger, route.Method+" "+route.Pattern)(route.Handler)
	handler = Deadline(options.RequestTimeout)(handler)
	handler = MaxBody(maxBody)(handler)
	handler = RateLimit(NewBucket(perSecond, burst, options.Now))(handler)
	return handler
}

// Handler exposes the composed handler for tests that do not need a listener.
func (s *Server) Handler() http.Handler { return s.server.Handler }

// Name identifies the component to the lifecycle runtime.
func (*Server) Name() string { return "auth-api-http" }

// Start binds the listener and serves in one owned goroutine.
//
// The goroutine is joined by Shutdown: http.Server.Serve returns
// http.ErrServerClosed once Shutdown runs, so the goroutine cannot outlive the
// component. Binding happens synchronously here so a port conflict fails
// startup instead of surfacing later as an asynchronous error.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return fmt.Errorf("api server is already started")
	}
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		// The address is this process's own configuration, not caller input,
		// so it is safe in the error; the underlying syscall error is not
		// wrapped in for the same reason net/http's own text is not needed.
		return fmt.Errorf("listen for auth API on %s: %w", s.server.Addr, err)
	}
	s.listener = listener
	s.server.BaseContext = func(net.Listener) context.Context { return ctx }
	go func() {
		if serveErr := s.server.Serve(listener); serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			select {
			case s.errors <- fmt.Errorf("auth API server: %w", serveErr):
			default:
			}
		}
	}()
	return nil
}

// Shutdown stops accepting and drains in-flight requests within the budget the
// lifecycle runtime allotted this component.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.RLock()
	started := s.listener != nil
	s.mu.RUnlock()
	if !started {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown auth API: %w", err)
	}
	return nil
}

// Errors lets the lifecycle runtime terminate when serving fails after bind.
func (s *Server) Errors() <-chan error { return s.errors }

// Address returns the bound address after Start. Port zero is resolved to the
// selected ephemeral port, which is what makes an end-to-end test
// deterministic without a fixed port.
func (s *Server) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// discard is an io.Writer sink for the default no-op logger.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
