package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type ServerOptions struct {
	Address  string
	Registry *Registry
	Service  string
	Version  string
}

// Server owns the worker's small operator-only HTTP surface.
type Server struct {
	registry *Registry
	service  string
	version  string

	server   *http.Server
	errors   chan error
	mu       sync.RWMutex
	listener net.Listener
}

func NewServer(options ServerOptions) (*Server, error) {
	if options.Registry == nil {
		return nil, fmt.Errorf("health registry is required")
	}
	if options.Address == "" {
		return nil, fmt.Errorf("health server address is required")
	}

	server := &Server{
		registry: options.Registry,
		service:  options.Service,
		version:  options.Version,
		errors:   make(chan error, 1),
	}
	server.server = &http.Server{
		Addr:              options.Address,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return server, nil
}

func (*Server) Name() string { return "operator-http" }

func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return fmt.Errorf("health server is already started")
	}
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("listen for operator HTTP: %w", err)
	}
	s.listener = listener
	s.server.BaseContext = func(net.Listener) context.Context { return ctx }
	go func() {
		if serveErr := s.server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.registry.SetLive(false)
			select {
			case s.errors <- fmt.Errorf("operator HTTP server: %w", serveErr):
			default:
			}
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.registry.SetReady(false)
	s.mu.RLock()
	started := s.listener != nil
	s.mu.RUnlock()
	if !started {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown operator HTTP: %w", err)
	}
	return nil
}

// Errors lets the lifecycle runtime terminate when serving fails after bind.
func (s *Server) Errors() <-chan error { return s.errors }

// Address returns the bound address after Start. Port zero is resolved to the
// selected ephemeral port, which makes smoke tests deterministic.
func (s *Server) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func (s *Server) handleHealth(response http.ResponseWriter, request *http.Request) {
	if !allowRead(response, request) {
		return
	}
	if !s.registry.Live() {
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{"status": "failed"})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReady(response http.ResponseWriter, request *http.Request) {
	if !allowRead(response, request) {
		return
	}
	status := s.registry.Readiness(request.Context())
	if !status.Ready {
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{
			"status":        "not_ready",
			"failed_checks": status.Failed,
		})
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleMetrics(response http.ResponseWriter, request *http.Request) {
	if !allowRead(response, request) {
		return
	}
	readiness := s.registry.Readiness(request.Context())
	ready := 0
	if readiness.Ready {
		ready = 1
	}
	live := 0
	if s.registry.Live() {
		live = 1
	}

	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(
		response,
		"# HELP dev_health_runtime_live Whether the process is live.\n"+
			"# TYPE dev_health_runtime_live gauge\n"+
			"dev_health_runtime_live %d\n"+
			"# HELP dev_health_runtime_ready Whether the process and required dependencies are ready.\n"+
			"# TYPE dev_health_runtime_ready gauge\n"+
			"dev_health_runtime_ready %d\n"+
			"# HELP dev_health_runtime_uptime_seconds Process uptime in seconds.\n"+
			"# TYPE dev_health_runtime_uptime_seconds gauge\n"+
			"dev_health_runtime_uptime_seconds %s\n"+
			"# HELP dev_health_runtime_required_checks Number of required readiness checks.\n"+
			"# TYPE dev_health_runtime_required_checks gauge\n"+
			"dev_health_runtime_required_checks %d\n"+
			"# HELP dev_health_runtime_info Build information for the process.\n"+
			"# TYPE dev_health_runtime_info gauge\n"+
			"dev_health_runtime_info{service=%s,version=%s} 1\n",
		live,
		ready,
		strconv.FormatFloat(s.registry.Uptime().Seconds(), 'f', 3, 64),
		s.registry.RequiredCount(),
		strconv.Quote(s.service),
		strconv.Quote(s.version),
	)
}

func allowRead(response http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		return true
	}
	response.Header().Set("Allow", "GET, HEAD")
	writeJSON(response, http.StatusMethodNotAllowed, map[string]any{"status": "method_not_allowed"})
	return false
}

func writeJSON(response http.ResponseWriter, status int, payload map[string]any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
