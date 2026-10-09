// Package main must NOT compile: the Atlassian Teams sink row takes a
// ProjectID, not a string.
package main

import "github.com/full-chaos/dev-health-ops/internal/atlassianteams"

func main() { _ = atlassianteams.OwnershipRow{ProjectID: "10001"} }
