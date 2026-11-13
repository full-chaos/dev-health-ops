// Package mintcli is the `mint` vertical of the dho binary: the edge-token
// minting helper and the signing-key file tool, folded from the standalone
// mint-edge-token and envelope-keys binaries (spec S1). The edge token is
// printed raw on stdout, per the exemption documented at spec S1 §3.4. This
// file only assembles the subpackages into one cli.Command tree.
package mintcli

import (
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/mintcli/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/mintcli/envelopekeys"
)

// Command is the `mint` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "mint",
		Kind:    cli.Group,
		Summary: "mint an edge token and print it on stdout, or generate the signing key files: edge-token, envelope-keys",
		Children: []cli.Command{
			envelopekeys.Command(),
			edgetoken.Command(),
		},
	}
}
