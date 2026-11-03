package benchmarking

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestMain runs the package on one ClickHouse server for the whole test
// binary (containers.RunWithSharedServers).
func TestMain(m *testing.M) {
	os.Exit(containers.RunWithSharedServers(m.Run))
}
