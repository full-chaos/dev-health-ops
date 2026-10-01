package pyjson_test

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
	"decode-body.golden.json":       "861b5ee34e4980f6fbea8e94a862eeafe29dcda6acb2cd27531e92a95ab82ba0",
	"dumps-default.golden.json":     "fffefe2f7f11729c8164ab1b544a5d1ac2eab7d3abd5e738853a8c5a0fe46d55",
	"marshal.golden.json":           "9f9630c0d074beec8a1404dfd5cc75db0cc8bd179a84f71fff634a80d8c7cf89",
	"model.golden.json":             "c41cca829ac0d4d9b61a8f428f7cf977c524bfa4ff0215680e7b14fa1b6ba821",
	"syntax-error-text.golden.json": "b55e358088bc547a5054d60f5d288476720eba9553e212b9750e78936cb584f9",
}

// goldens is the set of this package's frozen Python answers.
var goldens = programoracle.Set{
	Package:       "./internal/api/pyjson/",
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
