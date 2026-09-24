// Command dho is the Dev Health operator binary: one binary whose verticals
// (services and verb groups) are compiled in and listed below.
//
// This file stays a THIN ENTRYPOINT and must stay one: it lists the verticals
// and hands off to internal/cli. Dispatch, help, the version verb and the
// exit-code contract live in internal/cli; each vertical lives in its own
// package. main_test.go enforces the shape by parsing this file's AST.
//
// Adding a vertical is one line in commands. There is no init-time
// self-registration and no runtime plugin loading, so commands is the whole
// command tree.
package main

import (
	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/contractcheck"
	"github.com/full-chaos/dev-health-ops/internal/goapicli"
	"github.com/full-chaos/dev-health-ops/internal/mintcli"
	"github.com/full-chaos/dev-health-ops/internal/queryapiservice"
	"github.com/full-chaos/dev-health-ops/internal/reconcilerservice"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/schedulerservice"
	"github.com/full-chaos/dev-health-ops/internal/streamrunnerservice"
	"github.com/full-chaos/dev-health-ops/internal/workersctl"
	"github.com/full-chaos/dev-health-ops/internal/workerservice"
)

func main() { cli.Main("dho", commands()) }

// commands is the complete command tree, one vertical per line.
func commands() []cli.Command {
	return []cli.Command{
		admincli.Command(),
		apiservice.Command(),
		contractcheck.Command(),
		goapicli.Command(),
		mintcli.Command(),
		queryapiservice.Command(),
		reconcilerservice.Command(),
		rivermigrate.Command(),
		schedulerservice.Command(),
		streamrunnerservice.Command(),
		workerservice.Command(),
		workersctl.Command(),
	}
}
