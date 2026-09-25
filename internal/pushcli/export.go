package pushcli

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// runExport is `push export PROVIDER [--repo REPO]`: v1 registers no provider, so
// every name falls through to the "not implemented" message and exit 1 (an
// extension point kept so the subcommand exists in --help).
func runExport(_ context.Context, env cli.Env) int {
	flags := newFlags(env, "dho push export")
	flags.String("repo", "", "repository full name (provider-specific; unused by the stub)")
	positional, code, ok := parseArgs(flags, env, env.Args)
	if !ok {
		return code
	}
	if len(positional) != 1 {
		return usageError(env, "exactly one provider name is required")
	}
	fmt.Fprintf(env.Stderr, "error: `push export %s` is not implemented in v1 -- see CHAOS-2690 plan; "+
		"use `dev-hops push sample` + hand-written export, or the provider's native FullChaos sync instead.\n", positional[0])
	return exitDataFailure
}
