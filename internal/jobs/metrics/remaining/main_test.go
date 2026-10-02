package remaining

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// afterTests runs once the tests of this binary are done and RunTests has judged them. The integration build sets it
// (dora_ordering_contract_integration_test.go) to close the stores its tests opened; the default build leaves it nil.
// The TestMain is untagged and calls RunTests itself so every test binary of the package runs through it.
var afterTests func()

// TestMain fails the run when a test of this package that ran did not use its golden: a frozen answer no test compares is
// a comparison that stopped.
func TestMain(m *testing.M) {
	code := venueoracle.RunTests(m)
	if afterTests != nil {
		afterTests()
	}
	os.Exit(code)
}
