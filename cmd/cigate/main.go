// Command cigate runs the repository's CI gate programs.
//
//	cigate governance [flags]            PR test-evidence / risk-notes gate
//	cigate affected-integration [flags]  integration packages a change can affect
//	cigate govulncheck                   gate govulncheck JSON on stdin
//	cigate scope-candidates <module>     auth scope candidates from go list JSON on stdin
//	cigate metrics-proof [flags]         metrics.daily executed-proof readback against ClickHouse
package main

import (
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
	"github.com/full-chaos/dev-health-ops/internal/cigates/metricsproof"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cigate governance|affected-integration|govulncheck|scope-candidates|metrics-proof [args]")
		return 2
	}
	switch args[0] {
	case "governance":
		return cigates.Governance(args[1:], os.Stdout)
	case "affected-integration":
		return cigates.AffectedIntegration(args[1:], os.Stdout, os.Stderr)
	case "govulncheck":
		return cigates.AuthGovulncheck(os.Stdin, os.Stdout, os.Stderr)
	case "metrics-proof":
		return metricsproof.Run(args[1:], os.Getenv, metricsproof.OpenClickHouse, os.Stdout, os.Stderr)
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
