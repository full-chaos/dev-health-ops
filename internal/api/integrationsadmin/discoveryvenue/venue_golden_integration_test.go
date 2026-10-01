//go:build integration

package discoveryvenue

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/api/integrationsadmin/discoveryvenue/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			venuePythonBuild, test),
	}
}

// idSeq numbers the ids a test seeds, from 1: a frozen golden holds request
// bytes, and a recording and its replay must send the same ids.
var idSeq int

func nextID() uuid.UUID {
	idSeq++
	return uuid.MustParse(venueoracle.StableUUID(fmt.Sprintf("%s-%d", "discoveryvenue", idSeq)))
}
