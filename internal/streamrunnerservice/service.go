// Package streamrunnerservice is the stream runner service: `dho
// stream-runner --profile=<ingest|external|pagerduty>`, the long-running
// Valkey stream consumer that was the dev-health-stream-runner binary. Its
// shell.Spec keeps the service identity "dev-health-stream-runner", so the
// option registry (--profile), telemetry service.name and the posture
// guards see the same service as before the fold.
package streamrunnerservice

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

var streamRunnerSpec = shell.Spec{
	Service: "dev-health-stream-runner",
	// PagerDuty webhooks are an independently routable stream family (TRD 10.5)
	// with their own producer, DLQ, and receipt store, so they get a dedicated
	// profile rather than a third loop inside the ingest process.
	Profiles:                        []string{"ingest", "external", "pagerduty"},
	DefaultProfile:                  "ingest",
	ConfigureDependenciesWithLogger: configureStreamRunnerDependenciesWithLogger,
}

// Command is `dho stream-runner`.
func Command() cli.Command {
	return cli.Command{
		Name:    "stream-runner",
		Summary: "run a Valkey stream consumer profile (ingest, external, pagerduty)",
		Kind:    cli.Service,
		Run: func(ctx context.Context, env cli.Env) int {
			return shell.Execute(ctx, streamRunnerSpec, env.Args, env.Lookup, shell.IO{
				Stdout: env.Stdout,
				Stderr: env.Stderr,
			})
		},
	}
}
