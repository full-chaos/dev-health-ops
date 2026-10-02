//go:build integration

package sync

import (
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueGoldenPythonBuild is the build whose planner and discovery answered the
// venue goldens of this package: each program was executed there once, against
// a venue of that build.
const venueGoldenPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGoldenPins pins the SHA-256 of every venue golden under testdata/golden.
// The goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var venueGoldenPins = map[string]string{
	"venue-credential-fingerprint.golden.json": "16ac7a406486c1cacf68cb959f98b06d5272e89dca46289bddc18a8ce98e130f",
	"venue-jira-rename.golden.json":            "dd0580bc63de189c48f5d7d9182f648de1ee66d98cacd0977b9a8facb64cbb7f",
	"venue-zero-unit-stamp.golden.json":        "6f9d7bf6da4b08add7ed04fceab41e0dc4903203dd38355cf8021202acab924c",
}

// oracleDatabaseEnv names the variable that hands a program the address of the
// Python plane's database of this run. The address is new in every run, so it
// is not part of a request.
const oracleDatabaseEnv = "ORACLE_DATABASE_URI"

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
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	recipe := "git worktree add --detach $DIR " + venueGoldenPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); " +
		"then from the repository root: go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/scheduler/sync/ " +
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

// oracleDatabase is the per-run environment of a program that reads the Python
// plane's database of venue: its address, as SQLAlchemy takes it.
func oracleDatabase(t *testing.T, venue *venueoracle.Venue) func() map[string]string {
	t.Helper()
	return func() map[string]string {
		parsed, err := url.Parse(venue.AdminURI(t, venue.SourceDB))
		if err != nil {
			t.Fatal(err)
		}
		parsed.Scheme = "postgresql+psycopg2"
		return map[string]string{oracleDatabaseEnv: parsed.String()}
	}
}
