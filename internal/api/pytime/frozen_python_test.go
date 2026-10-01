package pytime_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonBuild is the build whose interpreter answered the frozen oracles of
// this package: each program was executed there once.
const pythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// producerIdentity is the producer those answers came from. json and
// pydantic-core are not source files of this repository, so the build above
// does not identify them: this does. A golden recorded by another interpreter
// or another pydantic-core is refused.
const producerIdentity = "python 3.14.7\nunicodedata 16.0.0\npydantic 2.13.5\npydantic_core 2.46.5"

// goldenPins pins the SHA-256 of every golden under testdata/golden. The
// goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var goldenPins = map[string]string{
	"datetime-reason.golden.json": "83d6f100d62bbc443dd9cadcbed90b239571ce1fefff0414a6dd33e427a1b98f",
	"fromisoformat.golden.json":   "e1941ef8297ac6cc11d9a9fcec7ff193d75b627518141219999a25c778421e76",
	"parse-date.golden.json":      "64fe55cd797b70c4497ef286cb0da81239835bdf636a4dc2b16d14f737f5b02d",
	"parse-datetime.golden.json":  "39b7f1758106b79b78e97d8616d1d6dece77d69b22c9eb5d0e33c0701f300942",
	"pydantic.golden.json":        "70a3b77f8d85f672928090df09b46a318661cb2dec998e60c3c8ada03abbceb7",
}

// goldens is the set of this package's frozen Python answers.
var goldens = programoracle.Set{
	Package:       "./internal/api/pytime/",
	Build:         pythonBuild,
	Identity:      producerIdentity,
	Distributions: []string{"pydantic", "pydantic_core"},
	Pins:          goldenPins,
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs: the answers were executed once on
// pythonBuild and are frozen in testdata/golden. A golden that is missing,
// edited, recorded for another program or input, or recorded by another
// producer than producerIdentity fails the test; so does a program that
// exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	return goldens.Outputs(t, repositoryRoot(t), golden, programs...)
}

// repositoryRoot is the repository root, from this file's own location.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
