package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

var streamRunnerSpec = shell.Spec{
	Service:               "dev-health-stream-runner",
	Profiles:              []string{"ingest", "external"},
	DefaultProfile:        "ingest",
	ConfigureDependencies: configureStreamRunnerDependencies,
}

func main() {
	shell.Main(streamRunnerSpec)
}
