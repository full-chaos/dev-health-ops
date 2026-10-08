package categorize

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestMain runs this package's tests through venueoracle.RunTests: the package
// holds a frozen golden (testdata/golden/batch.golden.json).
func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }
