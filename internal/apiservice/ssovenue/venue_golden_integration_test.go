//go:build integration

package ssovenue_test

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// runFloor and runCeiling bound the clock readings a run makes: a time at or
// after runFloor (after every time a test seeds, before any recording) and
// before runCeiling (before the earliest time a request supplies) was read from
// the clock during the run and is a placeholder in the golden on both planes.
var (
	runFloor   = time.Date(2026, time.September, 30, 0, 0, 0, 0, time.UTC)
	runCeiling = time.Date(2030, time.December, 1, 0, 0, 0, 0, time.UTC)
)

// venuePythonBuild is a build whose Python api still answered the routes the
// goldens under testdata/golden freeze: main when they were recorded, before the
// Python api was deleted.
const venuePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// venueGolden is the GoldenSpec of one oracle in this package: file is the
// golden's name under testdata/golden, test the oracle's function name, digest
// the SHA-256 the test pins ("PIN:<file>" until its first recording).
func venueGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/golden/" + file + ".json",
		PythonBuild: venuePythonBuild,
		SHA256:      digest,
		// The ids the test seeds have the shape of a random id and are compared by
		// value: only the ids the planes generate are blanked.
		Scrub: venueoracle.ScrubRunValues(runFloor, runCeiling, orgA, orgB, owner, admin, member, ownerB, samlID, oidcID, oauthID, otherOrgID, unknownID, badConfigID, badDomainsID),
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/ssovenue/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			venuePythonBuild, test),
	}
}
