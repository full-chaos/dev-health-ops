package pgmigrate_test

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// finishShared ends the package's shared PostgreSQL container (shared_instance_integration_test.go,
// integration tag only) and returns 1 when a test left server-wide state behind.
var finishShared func() int

// TestMain fails the run when a test of this package that ran did not use its
// golden: a frozen answer no test compares is a comparison that stopped.
func TestMain(m *testing.M) {
	code := venueoracle.RunTests(m)
	if finishShared != nil {
		if shared := finishShared(); code == 0 {
			code = shared
		}
	}
	os.Exit(code)
}
