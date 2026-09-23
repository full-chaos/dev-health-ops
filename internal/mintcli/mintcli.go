// Package mintcli is the `mint` vertical of the dho binary: the two
// credential-minting helpers, folded from the standalone mint-envelope and
// mint-edge-token binaries (spec S1). They stay for outside callers (the
// tools image, go-api-prove's own -proof-bearer-exec/-edge-bearer-exec) and
// print the raw credential on stdout, per the exemption documented at spec
// S1 §3.4. This file only assembles the two subpackages into one
// cli.Command tree.
package mintcli

import (
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/mintcli/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/mintcli/envelope"
)

// Command is the `mint` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "mint",
		Kind:    cli.Group,
		Summary: "mint a credential and print it on stdout: envelope, edge-token",
		Children: []cli.Command{
			envelope.Command(),
			edgetoken.Command(),
		},
	}
}
