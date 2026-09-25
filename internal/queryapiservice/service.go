// Package queryapiservice is `dho query-api`: the read-only Go query plane.
// It was the query-api binary. The service itself lives in
// internal/queryapi/server; this package only makes it a vertical of the dho
// command tree, handing the verb's arguments, environment lookup and streams
// to server.Run unchanged.
package queryapiservice

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

// Command is the `query-api` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "query-api",
		Summary: "serve the read-only Go query plane",
		Kind:    cli.Service,
		// It takes no arguments and ignores any it is given (server.Run), so a
		// where-it-acts root flag typed before it must be refused by the dispatcher.
		IgnoresArguments: true,
		Run: func(ctx context.Context, env cli.Env) int {
			return server.Run(ctx, env.Args, env.Lookup, env.Stdout, env.Stderr)
		},
	}
}
