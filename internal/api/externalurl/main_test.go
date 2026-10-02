package externalurl

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }
