package teamsidentity

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/egressallow"
)

func TestMain(m *testing.M) {
	egressallow.Loopback()
	os.Exit(m.Run())
}
