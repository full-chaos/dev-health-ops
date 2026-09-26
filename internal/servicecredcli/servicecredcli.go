// Package servicecredcli is the `service-credentials` group of dho: the verbs that
// create, list, rotate and revoke the internal service credentials the acr and
// worker-operator bearer checks read. The verbs live in package adminops (they share
// its database, flag and error helpers); this package is the group's own vertical for
// cmd/dho, which lists one Command() per vertical.
package servicecredcli

import (
	"github.com/full-chaos/dev-health-ops/internal/adminops"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Command is the `service-credentials` group.
func Command() cli.Command { return adminops.ServiceCredentialsCommand() }
