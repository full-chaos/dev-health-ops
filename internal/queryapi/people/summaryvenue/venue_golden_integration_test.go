//go:build integration

package summaryvenue

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueGoldenPythonBuild is the build whose Python answered the venue goldens
// of this package: each program was executed there once, against
// a venue of that build.
const venueGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGoldenPins pins the SHA-256 of every venue golden under testdata/golden.
// The goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var venueGoldenPins = map[string]string{
	"venue-people-summary-zone.golden.json": "PIN:venue-people-summary-zone.golden",
	"venue-people-summary.golden.json":      "PIN:venue-people-summary.golden",
}

// openVenueGolden opens the golden of the running venue test and returns it
// with the repository root the venue is built on: this checkout when frozen,
// the pinned checkout when recording. A golden that is not pinned fails the
// test: a frozen oracle never runs Python and never skips before it opened its
// golden.
func openVenueGolden(t *testing.T, name string) (*venueoracle.Golden, string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	recipe := "git worktree add --detach $DIR " + venueGoldenPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); " +
		"then from the repository root: go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/queryapi/people/summaryvenue/ " +
		"-test '^" + strings.SplitN(t.Name(), "/", 2)[0] + "$' -python-root $DIR"
	pin, pinned := venueGoldenPins[name]
	if !pinned {
		t.Fatalf("test %s has no frozen golden: venueGoldenPins names no %s. Add the entry with the value %q, then record: %s",
			t.Name(), name, "PIN:"+strings.TrimSuffix(name, ".json"), recipe)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name),
		PythonBuild: venueGoldenPythonBuild,
		SHA256:      pin,
		Recipe:      recipe,
	})
	return golden, golden.PythonRoot(t, root)
}
