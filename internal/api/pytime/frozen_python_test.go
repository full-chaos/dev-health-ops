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
	"datetime-reason.golden.json": "d7b742a910b2686c1d4a3696ed769f2d2d91de79d3ca32a5e0df511ed5854153",
	"fromisoformat.golden.json":   "b1e6c09092a43df05e9ad87cf2d89e010178c34a0b313a6b857b154cbc81d760",
	"parse-date.golden.json":      "805a5c02c897bd93461a14b4e78e9f9b89dff7b78e80fa53617dfe84ec224aba",
	"parse-datetime.golden.json":  "cad09250e8a040e3ed6368b8439cbc8660668077519f105fe91e4bf007403831",
	"pydantic.golden.json":        "a1d1ac45dae2634a67bc1d20453c07c68f054d31e63c2423490f27dbe0e47a46",
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
