package config_test

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The package holds a golden: its tests run through RunTests, which fails the
// run when a test that ran did not use its golden.
func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }
