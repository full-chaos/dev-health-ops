// Command cigate runs the repository's CI gate programs.
//
//	cigate governance [flags]            PR test-evidence / risk-notes gate
//	cigate plan-verdict [flags]          NONE / AFFECTED / FULL plan of a change set
//	cigate plan-reads [flags]            guard of the declared-read table of the plan
//	cigate govulncheck                   gate govulncheck JSON on stdin
//	cigate scope-candidates <module>     auth scope candidates from go list JSON on stdin
//	cigate metrics-proof [flags]         metrics.daily executed-proof readback against ClickHouse
//	cigate bigboy <verb> [args]          bigboy cut helpers: render-ch-users, grants-compare, pg-posture, bump-swap-values
//	cigate docs <verb> [args]            documentation, docs-delivery and commit-message gates
//	cigate plane-split-router [args]     bigboy traefik plane-split router from deploy values
//	cigate query-api-flags [args]        bigboy query-api env names from deploy values
//	cigate route-coverage [args]         ROUTEPROBE every routed path on the running Go planes
//	cigate web-env-parity [args]         web env-name parity between deploy values and compose.bigboy.yml
//	cigate live-e2e <verb>               live backend e2e harness checks and fixture steps
//	cigate release next-tag [flags]      next pre-release tag from the latest git tag and a package.json
package main

import (
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
	"github.com/full-chaos/dev-health-ops/internal/cigates/bigboyrender"
	"github.com/full-chaos/dev-health-ops/internal/cigates/docsprepare"
	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
	"github.com/full-chaos/dev-health-ops/internal/cigates/metricsproof"
	"github.com/full-chaos/dev-health-ops/internal/cigates/releasetag"
	"github.com/full-chaos/dev-health-ops/internal/cigates/routing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cigate governance|plan-verdict|plan-reads|govulncheck|scope-candidates|metrics-proof|bigboy|docs|docs-prepare|plane-split-router|query-api-flags|route-coverage|web-env-parity|live-e2e|release [args]")
		return 2
	}
	switch args[0] {
	case "governance":
		return cigates.Governance(args[1:], os.Stdout)
	case "plan-verdict":
		return affected.PlanVerdict(args[1:], os.Stdout, os.Stderr)
	case "plan-reads":
		return affected.PlanReads(args[1:], os.Stdout, os.Stderr)
	case "govulncheck":
		return cigates.AuthGovulncheck(os.Stdin, os.Stdout, os.Stderr)
	case "docs-prepare":
		return docsprepare.Run(args[1:], os.Stdout, os.Stderr)
	case "release":
		return releasetag.Run(args[1:], processEnv, os.Stdout, os.Stderr)
	case "live-e2e":
		return livee2e.Run(args[1:], processEnv, os.Stdout, os.Stderr)
	case "metrics-proof":
		return metricsproof.Run(args[1:], processEnv, metricsproof.OpenClickHouse, os.Stdout, os.Stderr)
	case "bigboy":
		return bigboyrender.Run(args[1:], os.Stdout, os.Stderr)
	case "docs":
		return docstools.Run(args[1:], os.Stdout, os.Stderr)
	case "plane-split-router":
		return routing.RunPlaneSplitRouter(args[1:], routing.OSDeps(), os.Stdout, os.Stderr)
	case "query-api-flags":
		return routing.RunQueryAPIFlags(args[1:], routing.OSDeps(), os.Stdout, os.Stderr)
	case "route-coverage":
		return routing.RunRouteCoverage(args[1:], routing.OSDeps(), os.Stdout, os.Stderr)
	case "web-env-parity":
		return routing.RunWebEnvParity(args[1:], routing.OSDeps(), os.Stdout, os.Stderr)
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
