//go:build integration

package apiservice

import (
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venuePythonBuild is a build whose Python api still answered the routes the
// goldens under testdata/venue freeze (the protected routes, external ingest,
// legacy ingest, webhook intake, customer push, the credential repository
// listings): main when they were recorded, before the Python api was deleted.
const venuePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGolden is the GoldenSpec of one venue oracle in this package: file is
// the golden's name under testdata/venue, test the oracle's function name,
// digest the SHA-256 the test pins ("PIN:<file>" until its first recording).
func venueGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/venue/" + file + ".json",
		PythonBuild: venuePythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			venuePythonBuild, test),
	}
}

// stableVenueID is the id a golden-backed seed gives the row it names: the
// recording run and every frozen run must seed the same ids, so a request or a
// response that carries one is the same bytes in both.
func stableVenueID(name string) uuid.UUID {
	return uuid.MustParse(venueoracle.StableUUID(name))
}

// ordered ranges over m in key order: a frozen golden holds the requests in the
// order the recording sent them, and a Go map ranges in a random order.
func ordered[V any](m map[string]V) iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for _, key := range slices.Sorted(maps.Keys(m)) {
			if !yield(key, m[key]) {
				return
			}
		}
	}
}
