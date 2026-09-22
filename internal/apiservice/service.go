// Package apiservice is `dho api`: the Go HTTP api that takes over the
// Python api's REST routes one area at a time.
//
// This package holds the service shell only. It mounts NO business route:
// every request gets the Python api's own 404 body until an area package
// adds routes. What runs today is the transport every future route inherits:
//
//   - the shared process runtime (internal/platform/shell): flags > env
//     configuration, the redacting JSON logger, OTEL, and the operator
//     listener with /healthz, /readyz and /metrics on --http-addr;
//   - a SEPARATE api listener on --api-addr, so probes never meet request
//     middleware;
//   - the transport middleware of internal/auth/httpapi (request id, panic
//     recovery, body bound, deadline), rendering every error in the Python
//     api's wire shape (errors.go);
//   - the Python api's security headers and CORS behaviour (headers.go,
//     cors.go), in the Python request order.
//
// Per-route policy (principal, org scope, impersonation, client rate limits,
// origin validation, licensing, audit) arrives with the first route that needs
// it; see the design spec's section 4.
package apiservice

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

const (
	// requestTimeout bounds every request context. The Python api has no
	// per-request bound; a route that needs a different one declares it when it
	// is ported.
	requestTimeout = 60 * time.Second
	// maxBodyBytes bounds every request body at the ingress in front of both
	// planes (proxy-body-size 50m on the prod ops ingress). The Python api has
	// no bound of its own, so any body the ingress forwards reaches it; this
	// default keeps that true for the Go api.
	maxBodyBytes = 50 << 20
	// maxHeaderBytes is the smallest bound under which the api accepts every
	// request head the Python api accepts. uvicorn on h11 (uv.lock), with the
	// head arriving in one piece, accepts a request line of up to 127,917
	// bytes of target and a single header value of up to 127,887 bytes;
	// net/http reads MaxHeaderBytes plus its own slop (measured +4,040 bytes
	// for a long target, +4,019 for a long header). The raw-HTTP golden pins
	// the edges.
	maxHeaderBytes = 123877
	// maxHeaderValueCount is high enough that the byte bound, not a count,
	// ends a request head, as in uvicorn, which has no count limit: the
	// shortest header line is 4 bytes, so 32,768 lines cannot fit the bound.
	maxHeaderValueCount = 32 << 10
	// listenerCheck is the readiness check that fails until the api listener
	// is bound.
	listenerCheck = "api_listener"
)

// Spec is the shell specification of `dho api`. Exported so a test can run
// the real service through shell.Execute.
var Spec = shell.Spec{
	Service:                         config.APIServiceName,
	Invocation:                      "dho api",
	TraceServiceName:                config.APIServiceName,
	ConfigureDependenciesWithLogger: configure,
}

// Command is the `api` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "api",
		Summary: "serve the Go HTTP api",
		Kind:    cli.Service,
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, Spec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
	}
}

// Routes is the route set the api mounts. It is empty: no business route is
// ported yet.
func Routes() []httpapi.Route { return nil }

func configure(
	_ context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	server, err := NewServer(cfg, logger, Routes())
	if err != nil {
		return nil, err
	}
	if err := registry.RegisterRequired(listenerCheck, func(context.Context) error {
		if server.Address() == "" {
			return errors.New("api listener is not bound")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return []lifecycle.Component{server}, nil
}

// NewServer builds the api listener with the full transport stack. The stack
// order, request side first, is: request id, panic recovery, security
// headers, CORS, then the mux (and, per route, recovery, deadline, body
// bound). It matches the Python api's request order for the middleware that
// exists here (src/dev_health_ops/api/_middleware.py registers in reverse).
func NewServer(cfg config.Config, logger *slog.Logger, routes []httpapi.Route) (*httpapi.Server, error) {
	return httpapi.NewServer(httpapi.ServerOptions{
		Name:           "api-http",
		Address:        cfg.APIAddress,
		Logger:         logger,
		Routes:         routes,
		RequestTimeout: requestTimeout,
		MaxBodyBytes:   maxBodyBytes,
		ErrorWriter:    WriteError,
		// The Python api echoes any non-empty X-Request-ID
		// (api/middleware/correlation_id.py) and routes the raw path, never
		// redirecting one.
		AcceptRequestID:     func(id string) bool { return id != "" },
		StrictPaths:         true,
		MaxHeaderBytes:      maxHeaderBytes,
		MaxHeaderValueCount: maxHeaderValueCount,
		Middleware: []func(http.Handler) http.Handler{
			SecurityHeaders,
			NewCORS(cfg.CORSAllowedOrigins).Wrap,
		},
	})
}
