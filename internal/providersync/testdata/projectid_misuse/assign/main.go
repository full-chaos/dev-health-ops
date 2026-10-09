// Package main must NOT compile: a string assigned to a ProjectID.
package main

import "github.com/full-chaos/dev-health-ops/internal/providersync"

func main() {
	var id providersync.ProjectID = "10001"
	_ = id
}
