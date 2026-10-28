package investment

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestMain fails the run when a test of this package that ran did not use its
// golden: a frozen answer no test compares is a comparison that stopped. The
// integration tests share one ClickHouse and one PostgreSQL server per binary.
func TestMain(m *testing.M) {
	os.Exit(containers.RunWithSharedServers(func() int { return venueoracle.RunTests(m) }))
}
