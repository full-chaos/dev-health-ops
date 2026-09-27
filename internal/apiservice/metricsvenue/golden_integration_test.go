//go:build integration

package metricsvenue

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonBuild is a build whose Python api still answered the routes this
// oracle drives that the Go api now serves (POST /admin/credentials/test and the
// integration source PATCH): the parent of the changes that reduced their Python
// bodies to the Go-served refusal (CHAOS-6845 and its sync-admin sibling). The
// golden under testdata is the answers and the /metrics exposition of that
// build's Python plane, EXECUTED (never authored) by the recipe its spec prints,
// and frozen by the digest the test pins.
const pythonBuild = "9642a8da15bb47d7400cef13cc93547b46e5f67a"

// goldenSpec is the GoldenSpec of one oracle: file is the golden's name under
// testdata, test the oracle's function name,
// digest the SHA-256 the test pins (empty only while recording).
func goldenSpec(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/" + file + ".json",
		PythonBuild: pythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s; then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/metricsvenue/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			pythonBuild, test),
	}
}
