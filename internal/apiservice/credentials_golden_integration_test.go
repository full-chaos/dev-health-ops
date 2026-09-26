//go:build integration

package apiservice

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// credentialsPythonBuild is a build whose Python api still answered the
// credential admin routes (list, get, create, update, delete, test connection):
// the parent of the change that reduced their Python bodies to the Go-served
// refusal (CHAOS-6845). The goldens under testdata/credentials are the answers
// of that build's Python plane, EXECUTED (never authored) by the recipe each
// golden's spec prints, and frozen by the digest each test pins.
const credentialsPythonBuild = "9642a8da15bb47d7400cef13cc93547b46e5f67a"

// credentialsGolden is the GoldenSpec of one credential oracle: file is the
// golden's name under testdata/credentials, test the oracle's function name,
// digest the SHA-256 the test pins (empty only while recording).
func credentialsGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/credentials/" + file + ".json",
		PythonBuild: credentialsPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s; then from the repository root: "+
			"DHO_VENUE_GOLDEN_UPDATE=1 DHO_VENUE_GOLDEN_PYTHON_ROOT=$DIR DEV_HEALTH_LIVE_PYTHON_ORACLES=1 "+
			"go test -tags=integration -count=1 -run '^%s$' ./internal/apiservice/ (it fails once by design and prints the digest to pin)",
			credentialsPythonBuild, test),
	}
}
