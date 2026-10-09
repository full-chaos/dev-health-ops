// Package main must NOT compile: a string converted to a ProjectID.
package main

import "github.com/full-chaos/dev-health-ops/internal/providersync"

func main() { _ = providersync.ProjectID("10001") }
