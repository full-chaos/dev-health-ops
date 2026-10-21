package restcore

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/egressallow"
)

func TestMain(m *testing.M) {
	egressallow.LocalTests()
	os.Exit(m.Run())
}
