package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// The body is Prometheus text built from bounded names and numbers, never
	// markup: tell the browser not to second-guess the declared type.
	response.Header().Set("X-Content-Type-Options", "nosniff")
	var output bytes.Buffer
	s.registry.WriteRuntimeMetrics(request.Context(), s.service, s.version, &output)
	// Degrade rather than fail: a source whose dependency is unreachable must
	// not take the process-level gauges above down with it, since live/ready/
	// uptime matter most while the process is unready. The per-source failure
	// gauge keeps that honest — a scraper can tell partial data from complete
	// data, and alert on the failure series, instead of silently reading a
	// short scrape as healthy.
	outcomes, err := s.registry.WriteMetricsPartial(&output)
	if err != nil {
		writeJSON(response, http.StatusServiceUnavailable, map[string]any{"status": "metrics_unavailable"})
		return
	}
	if len(outcomes) > 0 {
		WriteSourceFailedMetrics(&output, outcomes)
	}

	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(output.Bytes())
}

// WriteRuntimeMetrics writes the process-level block of /metrics: liveness,
// readiness, uptime, the required-check count, build info and one gauge per
// required readiness check. It is shared by the scrape handler and the OTLP
// bridge, so the two cannot disagree about what the process reports.
func (r *Registry) WriteRuntimeMetrics(ctx context.Context, service, version string, output io.Writer) {
	readiness := r.Readiness(ctx)
	ready := 0
	if readiness.Ready {
		ready = 1
	}
	live := 0
	if r.Live() {
		live = 1
	}

	_, _ = fmt.Fprintf(
		output,
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
		strconv.FormatFloat(r.Uptime().Seconds(), 'f', 3, 64),
		r.RequiredCount(),
		strconv.Quote(service),
		strconv.Quote(version),
	)
	// dev_health_runtime_ready collapses every required check into one bit, so
	// an alert fired from it cannot say which dependency is down — and a
	// crash-looping process serves no /readyz at all, leaving that one good
	// surface dark exactly when it is needed. Publish a labelled gauge per
	// required check alongside it so a scrape (and a page) can name the
	// failure. Every required check gets a series, passing or not, so an
	// alert can be written against the absence of failure, not the absence of
	// a series. readiness.Checks is sorted by name (Registry.CheckRequired),
	// so the label ordering is stable scrape to scrape.
	if len(readiness.Checks) > 0 {
		_, _ = fmt.Fprint(
			output,
			"# HELP dev_health_runtime_check_failed Whether a required readiness check is currently failing.\n"+
				"# TYPE dev_health_runtime_check_failed gauge\n",
		)
		for _, check := range readiness.Checks {
			failed := 0
			if check.Failed {
				failed = 1
			}
			// Name only, never dependency error text. Names are pre-registered
			// and validated against checkNamePattern (see
			// Registry.RegisterRequired), which is the same guarantee
			// dev_health_runtime_metrics_source_failed already relies on for
			// its source label below — reusing an already-reviewed pattern
			// rather than a new one.
			_, _ = fmt.Fprintf(
				output,
				"dev_health_runtime_check_failed{check=%s} %d\n",
				strconv.Quote(check.Name), failed,
			)
		}
	}
}

// WriteSourceFailedMetrics writes the per-source failure gauge for the given
// outcomes (nothing when there are none).
func WriteSourceFailedMetrics(output io.Writer, outcomes []MetricsSourceOutcome) {
	if len(outcomes) == 0 {
		return
	}
	_, _ = fmt.Fprint(
		output,
		"# HELP dev_health_runtime_metrics_source_failed Whether a registered metrics source failed to write its fragment for this scrape.\n"+
			"# TYPE dev_health_runtime_metrics_source_failed gauge\n",
	)
	for _, outcome := range outcomes {
		failed := 0
		if outcome.Err != nil {
			failed = 1
		}
		// The source NAME only. Outcome.Err is arbitrary dependency text
		// and has been observed to contain a database DSN, so it must never
		// reach this response; names are pre-registered and match
		// checkNamePattern, so they are safe as a label value unquoted.
		_, _ = fmt.Fprintf(
			output,
			"dev_health_runtime_metrics_source_failed{source=%s} %d\n",
			strconv.Quote(outcome.Source), failed,
		)
	}
}

// Scrape is the whole /metrics body of one process as fragments: the runtime
// block, every registered source, and the source-failure gauge. It is what the
// OTLP bridge reads, so everything a scrape sees is pushed.
type Scrape struct {
	Registry *Registry
	Service  string
	Version  string
}

// EachMetricsFragment yields the runtime fragment, then each registered source
// except those in skip, then the source-failure fragment. A skipped source is
// still written once to learn whether it failed: the failure fragment lists
// every registered source, as the scrape's does, so a skipped source's status
// is not lost; only its own fragment is not yielded.
func (s Scrape) EachMetricsFragment(skip map[string]bool, fn func(source string, fragment []byte, err error)) {
	var runtime bytes.Buffer
	s.Registry.WriteRuntimeMetrics(context.Background(), s.Service, s.Version, &runtime)
	fn("runtime", runtime.Bytes(), nil)
	var outcomes []MetricsSourceOutcome
	s.Registry.EachMetricsFragment(nil, func(source string, fragment []byte, err error) {
		outcomes = append(outcomes, MetricsSourceOutcome{Source: source, Err: err})
		if skip[source] {
			return
		}
		fn(source, fragment, err)
	})
	var failed bytes.Buffer
	WriteSourceFailedMetrics(&failed, outcomes)
	if failed.Len() > 0 {
		fn("runtime_source_failed", failed.Bytes(), nil)
	}
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
