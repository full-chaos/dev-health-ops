package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path"
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
	// Allow, set on the first route registered for a pattern, is the Allow
	// header of that pattern's 405. Empty means every registered method of
	// the pattern, sorted.
	Allow string
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
	// Name is the lifecycle component name and the label in this server's
	// own error messages. Empty means "auth-api-http".
	Name string
	// ErrorWriter renders every error this server emits. Nil means WriteError
	// (the ACP envelope).
	ErrorWriter ErrorWriter
	// Middleware wraps the whole mux, inside RequestID and the outermost
	// Recover, in the order given: Middleware[0] sees the request first. It is
	// for transport concerns every response carries (security headers, CORS);
	// per-route policy belongs on the Route.
	Middleware []func(http.Handler) http.Handler
	// AcceptRequestID decides whether an inbound X-Request-ID is reused. Nil
	// means this package's own narrow rule (acceptableRequestID).
	AcceptRequestID func(string) bool
	// StrictPaths answers, through ErrorWriter with CodeNotFound, every
	// request whose target the mux would otherwise redirect or reject: a path
	// that is not in canonical form ("/a/../b", "//x", "/a/./b"), a target
	// without a leading slash (CONNECT authority form), and "*" (the
	// server-wide OPTIONS handler is switched off so "OPTIONS *" reaches it).
	// Off (the default), the mux keeps net/http's own redirects.
	StrictPaths bool
	// MaxHeaderBytes bounds the request line plus headers. Zero means 64 KiB,
	// this package's own bound. Over it, net/http answers 431 itself.
	MaxHeaderBytes int
	// MaxHeaderValueCount bounds the number of header values. Zero means
	// net/http's default (500). Over it, net/http answers 431 itself.
	MaxHeaderValueCount int
	// IdleTimeout closes a keep-alive connection that has been idle this
	// long. Zero means 60 s, this package's own value.
	IdleTimeout time.Duration
	// ExplicitHead stops a GET route from also answering HEAD (net/http's
	// mux default): HEAD then gets the pattern's 405 unless a HEAD route is
	// registered for it.
	ExplicitHead bool
	// RedirectSlashes answers a request no route matches with Starlette's
	// redirect_slashes 307 when the same path with its trailing slash
	// toggled matches a route (for any method); see slashRedirector.
	RedirectSlashes bool
	// ForwardedAllowIPs is uvicorn's FORWARDED_ALLOW_IPS for the redirect's
	// scheme: a peer it trusts may set X-Forwarded-Proto. Nil means the
	// variable is unset, which is uvicorn's default, "127.0.0.1".
	ForwardedAllowIPs *string
}

// Server is the auth API listener. It is a lifecycle.Component so the runtime,
// not this package and not a package-level variable, owns its shutdown.
type Server struct {
	name   string
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

	name := options.Name
	if name == "" {
		name = "auth-api-http"
	}
	idleTimeout := options.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 60 * time.Second
	}
	maxHeaderBytes := options.MaxHeaderBytes
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = 1 << 16
	}
	server := &Server{name: name, logger: logger, errors: make(chan error, 1)}
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
		WriteTimeout:                 options.RequestTimeout + 5*time.Second,
		IdleTimeout:                  idleTimeout,
		DisableGeneralOptionsHandler: options.StrictPaths,
		// MaxHeaderBytes bounds header memory independently of the body
		// bound, which MaxBody cannot see.
		MaxHeaderBytes:      maxHeaderBytes,
		MaxHeaderValueCount: options.MaxHeaderValueCount,
		ErrorLog:            slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
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
	write := options.ErrorWriter
	if write == nil {
		write = WriteError
	}
	mux := http.NewServeMux()

	methodsByPattern := make(map[string][]string)
	allowByPattern := make(map[string]string)
	seen := make(map[string]struct{}, len(options.Routes))
	for _, route := range options.Routes {
		seen[route.Method+" "+route.Pattern] = struct{}{}
	}
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
		if _, exists := methodsByPattern[route.Pattern]; !exists && route.Allow != "" {
			allowByPattern[route.Pattern] = route.Allow
		}
		for _, method := range methodsByPattern[route.Pattern] {
			if method == route.Method {
				return nil, fmt.Errorf("route %s is registered twice", key)
			}
		}
		methodsByPattern[route.Pattern] = append(methodsByPattern[route.Pattern], route.Method)
		mux.Handle(key, routeChain(route, options, logger, write))
	}

	for pattern, methods := range methodsByPattern {
		sort.Strings(methods)
		allow := strings.Join(methods, ", ")
		if explicit, ok := allowByPattern[pattern]; ok {
			allow = explicit
		}
		notAllowed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allow)
			write(w, r, CodeMethodNotAllowed)
		})
		mux.Handle(pattern, notAllowed)
		// A GET pattern also matches HEAD in net/http's mux; the more
		// specific HEAD pattern registered here takes it back.
		_, hasGet := seen[http.MethodGet+" "+pattern]
		_, hasHead := seen[http.MethodHead+" "+pattern]
		if options.ExplicitHead && hasGet && !hasHead {
			mux.Handle(http.MethodHead+" "+pattern, notAllowed)
		}
	}

	var redirector *slashRedirector
	if options.RedirectSlashes {
		patterns := make([]string, 0, len(methodsByPattern))
		for pattern := range methodsByPattern {
			patterns = append(patterns, pattern)
		}
		allow := "127.0.0.1"
		if options.ForwardedAllowIPs != nil {
			allow = *options.ForwardedAllowIPs
		}
		redirector = newSlashRedirector(patterns, ParseForwardedTrust(allow))
	}
	notFound := func(w http.ResponseWriter, r *http.Request) {
		if redirector != nil && redirector.redirect(w, r) {
			return
		}
		write(w, r, CodeNotFound)
	}
	mux.Handle("/", http.HandlerFunc(notFound))

	// RequestID is outermost so every response -- including the 404 and 405
	// envelopes above, which never reach a route chain -- carries a
	// correlation id. The outer Recover covers the mux itself and those two
	// handlers; a route's own Recover (inside routeChain) catches first and
	// logs the real pattern.
	var handler http.Handler = mux
	if redirector != nil {
		handler = redirector.wrap(mux, notFound)
	}
	if options.StrictPaths {
		// Inside the middleware, so a refused target still gets every
		// transport header a routed response gets.
		handler = strictPaths(handler, notFound)
	}
	for index := len(options.Middleware) - 1; index >= 0; index-- {
		if options.Middleware[index] == nil {
			return nil, fmt.Errorf("middleware %d is nil", index)
		}
		handler = options.Middleware[index](handler)
	}
	accept := options.AcceptRequestID
	if accept == nil {
		accept = acceptableRequestID
	}
	return RequestIDWith(accept)(RecoverWith(logger, "<unrouted>", write)(handler)), nil
}

// routeChain wraps one route's handler. Order is deliberate: rate limiting is
// the cheapest rejection and runs first, then the body bound (which can reject
// on Content-Length without reading anything), then the deadline, then the
// route's own panic recovery closest to the handler so the log line names the
// real pattern.
func routeChain(route Route, options ServerOptions, logger *slog.Logger, write ErrorWriter) http.Handler {
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

	handler := RecoverWith(logger, route.Method+" "+route.Pattern, write)(route.Handler)
	handler = Deadline(options.RequestTimeout)(handler)
	handler = MaxBodyWith(maxBody, write)(handler)
	handler = RateLimitWith(NewBucket(perSecond, burst, options.Now), write)(handler)
	return handler
}

// Handler exposes the composed handler for tests that do not need a listener.
func (s *Server) Handler() http.Handler { return s.server.Handler }

// Name identifies the component to the lifecycle runtime.
func (s *Server) Name() string { return s.name }

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
		return fmt.Errorf("listen for %s on %s: %w", s.name, s.server.Addr, err)
	}
	s.listener = listener
	s.server.BaseContext = func(net.Listener) context.Context { return ctx }
	go func() {
		if serveErr := s.server.Serve(listener); serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			select {
			case s.errors <- fmt.Errorf("%s server: %w", s.name, serveErr):
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
		return fmt.Errorf("shutdown %s: %w", s.name, err)
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

// strictPaths answers NotFound for every request target net/http's ServeMux
// would not route as given. ServeMux cleans the escaped path and redirects
// when the cleaned form differs, and answers "*" with a bare 400; a service
// that must answer exactly like one that routes the raw path (the Go api
// mirrors Starlette, which never rewrites a path) stops those requests here.
// The check is the mux's own rule (net/http cleanPath over EscapedPath), so
// every request that passes it is one the mux routes without redirecting.
func strictPaths(next http.Handler, notFound http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escaped := r.URL.EscapedPath()
		if r.RequestURI == "*" || !strings.HasPrefix(escaped, "/") || canonicalPath(escaped) != escaped {
			notFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// canonicalPath is net/http's cleanPath: path.Clean, keeping a trailing
// slash.
func canonicalPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if p[len(p)-1] == '/' && cleaned != "/" {
		cleaned += "/"
	}
	return cleaned
}
