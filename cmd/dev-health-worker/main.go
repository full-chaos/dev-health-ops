package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

var workerSpec = shell.Spec{
	Service:               "dev-health-worker",
	Profiles:              []string{"latency", "sync", "heavy", "ops"},
	DefaultProfile:        "latency",
	ConfigureDependencies: configureWorkerDependencies,
}

func main() {
	shell.Main(workerSpec)
}
