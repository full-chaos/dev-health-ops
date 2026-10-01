//go:build integration

package admin_test

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// adminPythonBuild is a build whose Python api still answered the admin routes
// the goldens under testdata/admin freeze (users, organisations, invites,
// impersonation, org deletion, LLM settings status, the admin rate limits): main
// when they were recorded, before the Python admin routers were deleted.
const adminPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// adminGolden is the GoldenSpec of one admin venue oracle: file is the golden's
// name under testdata/admin, test the oracle's function name, digest the SHA-256
// the test pins ("PIN:<file>" until its first recording).
func adminGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/admin/" + file + ".json",
		PythonBuild: adminPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/admin/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			adminPythonBuild, test),
	}
}
