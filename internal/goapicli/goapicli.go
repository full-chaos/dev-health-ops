// Package goapicli is the `goapi` vertical of the dho binary: the Go-API
// rollout operator surface, folded from the standalone go-api-routing,
// go-api-prove and go-api-rest-prove binaries (spec S1). Each old binary
// moved to its own subpackage (routing, prove, restprove) with its logic
// unchanged; this file only assembles them into one cli.Command tree.
package goapicli

import (
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/goapicli/prove"
	"github.com/full-chaos/dev-health-ops/internal/goapicli/restprove"
	"github.com/full-chaos/dev-health-ops/internal/goapicli/routing"
)

// Command is the `goapi` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "goapi",
		Kind:    cli.Group,
		Summary: "Go-API rollout operator surface: routing, prove, prove-write, rest-prove",
		Children: []cli.Command{
			routing.Command(),
			prove.Command(),
			prove.WriteCommand(),
			restprove.Command(),
		},
	}
}
