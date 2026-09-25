// Package workerservice is the River worker service: `dho worker`, the
// long-running process that consumes the queues its deployment selects. It
// was the dev-health-worker binary; its shell.Spec keeps the service identity
// "dev-health-worker", so the option registry (--queues,
// --preclaim-readiness-timeout), telemetry service.name, the posture guards
// and the deployment contract see the same service as before the fold.
package workerservice

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

// A worker consumes only the registered queues selected by its deployment.
// Queue selection is explicit and static for the process lifetime; deployment
// topology is not encoded as an application profile.
var workerSpec = shell.Spec{
	Service:                         "dev-health-worker",
	RequireQueues:                   true,
	ConfigureDependenciesWithLogger: configureWorkerDependenciesWithLogger,
}

// Command is `dho worker`.
func Command() cli.Command {
	return cli.Command{
		Name:    "worker",
		Summary: "consume the River queues this deployment selects",
		Kind:    cli.Service,
		// dev-hops's root --log-level, typed before the command, is this service's own flag.
		RootFlags: []cli.RootFlag{cli.RootLogLevel},
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, workerSpec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
	}
}
