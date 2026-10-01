package apiservice

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// programPythonBuild is a build whose Python api still carried the code the
// program goldens under testdata/program were executed against (the api's CORS
// middleware, its route table and its request models): main when they were
// recorded, before the Python api was deleted.
const programPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// programGolden is the GoldenSpec of one Python-program oracle in this package:
// file is the golden's name under testdata/program, test the oracle's function
// name, digest the SHA-256 the test pins ("PIN:<file>" until its first
// recording).
func programGolden(file, test, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/program/" + file + ".json",
		PythonBuild: programPythonBuild,
		SHA256:      digest,
		Recipe: fmt.Sprintf("git worktree add --detach $DIR %s (with its .venv: uv sync --frozen --no-install-project); then from the repository root: "+
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/ -test '^%s$' -python-root $DIR "+
			"(records on the pinned build, replays the candidate in a fresh process, and only then promotes it and pins its digest)",
			programPythonBuild, test),
	}
}

// producerEnv is the environment entries that shape a producer's answer. They
// are part of the program's request, and with PATH, HOME, the pinned sources on
// PYTHONPATH and no bytecode they are the whole environment the producer gets:
// nothing is inherited from the process that records.
var producerEnv = map[string]string{"PYTHONHASHSEED": "0", "PYTHONUTF8": "1"}

// producerCommandEnv is the environment a recording gives the producer's child.
func producerCommandEnv(pinnedRoot string) []string {
	environment := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"PYTHONPATH=" + filepath.Join(pinnedRoot, "src"), "PYTHONDONTWRITEBYTECODE=1",
	}
	names := make([]string, 0, len(producerEnv))
	for name := range producerEnv {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		environment = append(environment, name+"="+producerEnv[name])
	}
	return environment
}

// withoutLogLines drops the structured log lines the Python process writes on
// start-up ({"timestamp": ...}): they carry the clock, so they would make two
// recordings differ, and no test reads them.
func withoutLogLines(output []byte) string {
	var kept []string
	for _, line := range strings.SplitAfter(string(output), "\n") {
		if !strings.HasPrefix(line, `{"timestamp": "`) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}
