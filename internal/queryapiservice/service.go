// Package queryapiservice is `dho query-api`: the read-only Go query plane on the
// shared process runtime (internal/platform/shell), like every other dho service:
//
//   - options are flag > environment through the config registry
//     (internal/platform/config, dev-health-query-api), an unknown flag or a
//     positional argument exits 2, and --help is rendered from that registry;
//   - the operator listener on --http-addr (:8080) serves /healthz, /readyz and
//     /metrics, and owns the redacting JSON logger, tracing and the OTel meter
//     provider;
//   - the query routes (/query, /registry, /buildinfo, /api/v1/*) are served by
//     lifecycle components on --query-addr (QUERY_API_ADDR, :8090) and, when
//     --internal-addr is set, on the internal listener that honours the
//     X-DH-Internal-* identity headers.
//
// The routes themselves are internal/queryapi/server.Build: this package only
// builds them from the declared settings and hands the shell their listeners and
// their readiness checks.
package queryapiservice

import (
	"context"
	"errors"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

// Spec is the shell specification of `dho query-api`. Exported so a test can run
// the real service through shell.Execute.
var Spec = shell.Spec{
	Service:                         config.QueryAPIServiceName,
	Invocation:                      "dho query-api",
	TraceServiceName:                config.QueryAPIServiceName,
	ConfigureDependenciesWithLogger: configure,
}

// Command is the `query-api` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "query-api",
		Summary: "serve the read-only Go query plane",
		Kind:    cli.Service,
		// dev-hops's root --log-level, typed before the command, is this service's own flag.
		RootFlags: []cli.RootFlag{cli.RootLogLevel},
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, Spec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
	}
}

// listenerCheck is the readiness check that fails until the query listener is bound.
const listenerCheck = "query_listener"

func configure(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	plane, err := server.Build(func(name string) string {
		value, _ := cfg.Setting(name)
		return value
	})
	if err != nil {
		// The error names a route and its configuration, never a secret, but the
		// shell's rule is that a dependency error is not assumed free of DSNs:
		// log the failure with the redacting logger and stop.
		logger.ErrorContext(ctx, "build query routes", "error", err)
		return nil, err
	}
	registered := false
	closeOnError := func(err error) ([]lifecycle.Component, error) {
		plane.Close()
		return nil, err
	}
	// One required check per dependency class, so the operator /readyz names the
	// failing one; a deployment with no /query configured has one explicit passing
	// check (the registry fails closed on none).
	for _, probe := range plane.Probes {
		if err := registry.RegisterRequired(probe.Name, server.ObserveProbe(probe)); err != nil {
			return closeOnError(err)
		}
		registered = true
	}
	if !registered {
		if err := registry.RegisterRequired("query_routes", server.NotConfiguredCheck()); err != nil {
			return closeOnError(err)
		}
	}
	// One release of compatibility (D2627): /healthz, /readyz and /metrics are also
	// answered on the query listener, in the old shapes, so the chart's probes and
	// the scrape can move to the operator listener in their own deploy change.
	// The handler is only borrowed (never started): it reads the same registry.
	operatorRoutes, err := health.NewServer(health.ServerOptions{
		Address:  "127.0.0.1:0",
		Registry: registry,
		Service:  config.QueryAPIServiceName,
	})
	if err != nil {
		return closeOnError(err)
	}
	logger.WarnContext(ctx, "the query listener still serves /healthz, /readyz and /metrics for one release; probes and scrape belong on the operator listener", "setting", "HTTP_ADDR")
	public, internal := server.Listeners(cfg.QueryAPIAddress, cfg.QueryAPIInternalAddress, plane, server.OperatorCompat(registry, operatorRoutes, config.QueryAPIServiceName))
	if err := registry.RegisterRequired(listenerCheck, func(context.Context) error {
		if public.Address() == "" {
			return errors.New("query listener is not bound")
		}
		return nil
	}); err != nil {
		return closeOnError(err)
	}
	components := []lifecycle.Component{server.Closer{Plane: plane}, public}
	if internal != nil {
		components = append(components, internal)
	} else {
		logger.WarnContext(ctx, "no internal listener: X-DH-Internal-* identity headers are honoured nowhere", "setting", "QUERY_API_INTERNAL_ADDR")
	}
	return components, nil
}
