//go:build integration

package admin_test

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// governancePythonBuild is a build whose Python api still answered the
// retention, IP-allowlist, settings and LLM-settings admin routes: the parent
// of the change that reduced their Python bodies to the Go-served refusal
// (CHAOS-6844). The goldens under testdata/governance are the answers of that
// build's Python plane, EXECUTED (never authored) by the recipe each golden's
// spec prints, and frozen by the digest each test pins.
const governancePythonBuild = "9642a8da15bb47d7400cef13cc93547b46e5f67a"

// governanceGolden is the GoldenSpec of one venue oracle of those routes: file
// is the golden's name under testdata/governance, test the oracle's function
// name, digest the SHA-256 the test pins (empty only while recording).
func governanceGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/governance/" + file + ".json",
		PythonBuild: governancePythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s; then from the repository root: "+
			"DHO_VENUE_GOLDEN_UPDATE=1 DHO_VENUE_GOLDEN_PYTHON_ROOT=$DIR DEV_HEALTH_LIVE_PYTHON_ORACLES=1 "+
			"go test -tags=integration -count=1 -run '^%s$' ./internal/apiservice/admin/ (it fails once by design and prints the digest to pin)",
			governancePythonBuild, test),
	}
}

// goldenIDs returns a generator of deterministic ids for one oracle: the n-th
// id of a prefix is the same in every process, which a golden needs (a
// recording run and a frozen run are different processes) wherever an id reaches
// a compared response, row or request path. The ids must be drawn in the same
// order in both runs.
func goldenIDs(prefix string) func() uuid.UUID {
	next := 0
	return func() uuid.UUID {
		next++
		return uuid.MustParse(venueoracle.StableUUID(prefix + "-" + strconv.Itoa(next)))
	}
}
