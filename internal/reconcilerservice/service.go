// Package reconcilerservice is the reconciler service: `dho reconciler`, the
// long-running process that relays the job outbox into River and runs the
// sync reconciler stages, and `dho reconciler healthcheck`, its exec-form
// readiness probe. It was the dev-health-reconciler binary; its shell.Spec
// keeps the service identity "dev-health-reconciler", so the option registry
// (--unreclaimable-sweep), telemetry service.name, the posture guards and the
// deployment contract see the same service as before the fold.
package reconcilerservice

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

// CHAOS-4005: the reconciler owns the unreclaimable-dispatching sweep. Its
// --unreclaimable-sweep flag is declared in the platform option registry scoped
// to this service name (CHAOS-4020), so the flag, its --help entry, its
// SYNC_UNRECLAIMABLE_SWEEP fallback, and flag > env precedence all follow from
// one declaration instead of a per-binary opt-in bool.
var reconcilerSpec = shell.Spec{
	Service:                         "dev-health-reconciler",
	ConfigureDependenciesWithLogger: configureReconcilerDependenciesWithLogger,
}

// Command is `dho reconciler`, with `dho reconciler healthcheck` as its one
// child. The healthcheck is a child verb, not a flag registered through the
// shared option registry: it must exit fast rather than boot the whole
// dependency graph, and it is invoked by Docker's own healthcheck exec, never
// a human.
func Command() cli.Command {
	return cli.Command{
		Name:    "reconciler",
		Summary: "relay the job outbox into River and run the sync reconciler stages",
		Kind:    cli.Service,
		// dev-hops's root --log-level, typed before the command, is this service's own flag.
		RootFlags: []cli.RootFlag{cli.RootLogLevel},
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, reconcilerSpec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
		Children: []cli.Command{{
			Name:    "healthcheck",
			Summary: "exit 0 when this host's reconciler answers /readyz with 200, else 1",
			Kind:    cli.Verb,
			Run: func(_ context.Context, env cli.Env) int {
				if len(env.Args) != 0 {
					fmt.Fprintf(env.Stderr, "{\"error\":{\"code\":\"invalid_request\",\"detail\":\"healthcheck takes no arguments\"}}\n")
					return 2
				}
				return runHealthcheck(env.Lookup)
			},
		}},
	}
}

// runHealthcheck is CHAOS-4239's Compose healthcheck probe: the runtime
// image is gcr.io/distroless/static-debian12 (docker/go-worker.Dockerfile),
// which has no shell and no curl/wget, so Docker's exec-form healthcheck
// must invoke this binary itself rather than a CMD-SHELL one-liner. It
// queries the SAME process's own /readyz over loopback and mirrors that
// status code as its exit code -- readyz already aggregates every
// syncreconciler stage's health.Registry check (CHAOS-4239's degraded-stage
// and step-overrun checks included), so this adds no new failure logic, only
// a way for something outside the process to observe it. A stage that
// ignores its own context and never returns (the ticket's round-3 finding)
// stops answering readyz the same way it stops answering everything else;
// this is what turns that into a container Docker can see as unhealthy and,
// with a restart policy or an external supervisor watching health status,
// actually recover from -- the mirror image of "any stage kills the
// process" is "a wedged process is never restarted," and this closes it.
//
// It deliberately does not import net/http/httptest or reuse
// internal/platform/health's client-side helpers: this is a tiny,
// dependency-free probe that must keep working even if something else in
// the binary's health machinery is broken.
func runHealthcheck(lookup secrets.LookupEnv) int {
	addr, _ := lookup("DEV_HEALTH_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%s/readyz", port))
	if err != nil {
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func configureReconcilerDependenciesWithLogger(
	ctx context.Context,
	cfg config.Config,
	registry *health.Registry,
	logger *slog.Logger,
) ([]lifecycle.Component, error) {
	return configureReconcilerDependenciesWithSourcesAndLogger(
		ctx,
		cfg,
		registry,
		logger,
		productionReconcilerDependencySources,
	)
}
