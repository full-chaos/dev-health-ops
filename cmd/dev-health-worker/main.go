package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

// A worker consumes only the registered queues selected by its deployment.
// Queue selection is explicit and static for the process lifetime; deployment
// topology is not encoded as an application profile.
var workerSpec = shell.Spec{
	Service:                         "dev-health-worker",
	RequireQueues:                   true,
	ConfigureDependenciesWithLogger: configureWorkerDependenciesWithLogger,
}

func main() {
	shell.Main(workerSpec)
}
