package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

// The worker has no default profile. Every profile it accepts owns registered
// job kinds, and an operator must name the one this process runs: starting a
// River consumer with nothing registered is unrepresentable rather than
// permanently unready. The empty "latency" profile was removed with CUT-02
// because it could never satisfy exact startup validation.
var workerSpec = shell.Spec{
	Service:                         "dev-health-worker",
	Profiles:                        []string{"sync", "heavy", "ops"},
	ConfigureDependenciesWithLogger: configureWorkerDependenciesWithLogger,
}

func main() {
	shell.Main(workerSpec)
}
