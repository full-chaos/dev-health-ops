// Command cigate runs the repository's CI gate programs.
//
//	cigate governance [flags]            PR test-evidence / risk-notes gate
//	cigate affected-integration [flags]  integration packages a change can affect
//	cigate govulncheck                   gate govulncheck JSON on stdin
//	cigate scope-candidates <module>     auth scope candidates from go list JSON on stdin
//	cigate metrics-proof [flags]         metrics.daily executed-proof readback against ClickHouse
//	cigate live-e2e <verb>               live backend e2e harness checks and fixture steps
package main

import (
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
	"github.com/full-chaos/dev-health-ops/internal/cigates/metricsproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cigate governance|affected-integration|govulncheck|scope-candidates|metrics-proof|live-e2e [args]")
		return 2
	}
	switch args[0] {
	case "governance":
		return cigates.Governance(args[1:], os.Stdout)
	case "affected-integration":
		return cigates.AffectedIntegration(args[1:], os.Stdout, os.Stderr)
	case "govulncheck":
		return cigates.AuthGovulncheck(os.Stdin, os.Stdout, os.Stderr)
	case "live-e2e":
		return livee2e.Run(args[1:], processEnv, os.Stdout, os.Stderr)
	case "metrics-proof":
		return metricsproof.Run(args[1:], processEnv, metricsproof.OpenClickHouse, os.Stdout, os.Stderr)
	case "scope-candidates":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: cigate scope-candidates <module>")
			return 2
		}
		return cigates.AuthScopeCandidates(args[1], os.Stdin, os.Stdout, os.Stderr)
	}
	fmt.Fprintf(os.Stderr, "cigate: unknown command %q\n", args[0])
	return 2
}

// processEnv reads the environment through the registry, so a DSN password never reaches the process logger.
func processEnv(name string) string {
	value, _ := secrets.ProcessLookup(name)
	return value
}
