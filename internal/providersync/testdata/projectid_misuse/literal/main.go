// Package main must NOT compile: a ProjectID built from a literal.
package main

import "github.com/full-chaos/dev-health-ops/internal/providersync"

func main() { _ = providersync.ProjectID{"10001"} }
