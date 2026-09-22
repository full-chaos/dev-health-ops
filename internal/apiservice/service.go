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
	// maxBodyBytes bounds every request body. The Python api has no global
	// bound; a route that needs more declares its own MaxBodyBytes when it is
	// ported.
	maxBodyBytes = 16 << 20
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
		Middleware: []func(http.Handler) http.Handler{
			SecurityHeaders,
			NewCORS(cfg.CORSAllowedOrigins).Wrap,
		},
	})
}
