package fixturescli

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestMain fails the run when a test of this package that ran did not use its
// golden: a frozen answer no test compares is a comparison that stopped.
//
// The tests of this package share one PostgreSQL, one ClickHouse and one Valkey server per test binary
// (containers.RunWithSharedServers; CHAOS-11783): each test gets a private database on them.
func TestMain(m *testing.M) {
	os.Exit(containers.RunWithSharedServers(func() int { return venueoracle.RunTests(m) }))
}
