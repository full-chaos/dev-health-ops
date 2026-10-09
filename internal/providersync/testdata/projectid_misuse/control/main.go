// Package main MUST compile: the same row, built through the constructor.
// It proves the build harness of the misuse cases can pass.
package main

import (
	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func main() {
	id, ok := providersync.JiraProjectID("10001")
	if !ok {
		return
	}
	_ = atlassianteams.OwnershipRow{ProjectID: id}
}
