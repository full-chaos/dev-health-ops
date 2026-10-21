package externalingest

import (
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// programPythonBuild is a build whose Python api still carried the code the
// program goldens under testdata/golden were executed against (the external-ingest schema bundle): main
// when they were recorded, before the Python api was deleted.
const programPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// programGolden is the GoldenSpec of one Python-program oracle in this package:
// file is the golden's name under testdata/golden, test the oracle's function
// name, digest the SHA-256 the test pins ("PIN:<file>" until its first
// recording).
func programGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/golden/" + file + ".json",
		PythonBuild: programPythonBuild,
		SHA256:      digest,
	}
}
