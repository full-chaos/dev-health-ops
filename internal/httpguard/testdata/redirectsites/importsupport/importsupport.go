// Package importsupport is a fixture of the site walker: a production package that imports a test-support package (whose
// clients are outside the walk) is a problem.
package importsupport

import "github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"

var _ = redirectprobe.Reach
