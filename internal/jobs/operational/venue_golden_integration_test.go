//go:build integration

package operational

import (
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueGoldenPythonBuild is the build whose Python answered the venue goldens
// of this package: each program was executed there once, against
// a venue of that build.
const venueGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGoldenPins pins the SHA-256 of every venue golden under testdata/golden.
var venueGoldenPins = map[string]string{
	"venue-webhook-handoff.golden.json": "47a15c492d08be34b3576aac259c1a365dd5935a64af9edf916e665e736962f9",
}

// openVenueGolden opens the golden of the running venue test and returns it
// with the repository root the venue is built on. A golden that is not pinned fails the
// test: a frozen oracle never runs Python and never skips before it opened its
// golden.
func openVenueGolden(t *testing.T, name string) (*venueoracle.Golden, string) {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	pin, pinned := venueGoldenPins[name]
	if !pinned {
		t.Fatalf("test %s has no frozen golden: venueGoldenPins names no %s. Add the entry with the pinned digest",
			t.Name(), name)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name),
		PythonBuild: venueGoldenPythonBuild,
		SHA256:      pin,
	})
	return golden, root
}
