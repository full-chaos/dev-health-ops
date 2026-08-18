package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

var streamRunnerSpec = shell.Spec{
	Service: "dev-health-stream-runner",
	// PagerDuty webhooks are an independently routable stream family (TRD 10.5)
	// with their own producer, DLQ, and receipt store, so they get a dedicated
	// profile rather than a third loop inside the ingest process.
	Profiles:                        []string{"ingest", "external", "pagerduty"},
	DefaultProfile:                  "ingest",
	ConfigureDependenciesWithLogger: configureStreamRunnerDependenciesWithLogger,
}

func main() {
	shell.Main(streamRunnerSpec)
}
