//go:build integration

package externalingestvenue

import (
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venuePythonBuild is a build whose Python api still answered the external
// ingest routes the goldens under testdata freeze: main when they were
// recorded, before the Python api was deleted.
const venuePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGolden is the GoldenSpec of one oracle in this package: file is the
// golden's name under testdata, test the oracle's function name, digest the
// SHA-256 the test pins ("PIN:<file>" until its first recording).
func venueGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/" + file + ".json",
		PythonBuild: venuePythonBuild,
		SHA256:      digest,
	}
}
