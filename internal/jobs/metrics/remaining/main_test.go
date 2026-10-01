//go:build !integration

package remaining

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestMain fails the run when a test of this package that ran did not use its
// golden: a frozen answer no test compares is a comparison that stopped. (The
// integration build has its own TestMain, which also closes its stores.)
func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }
