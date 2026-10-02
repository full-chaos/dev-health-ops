package pyjson_test

import (
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
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
	"decode-body.golden.json":       "a5b797330fcdf7f40ff87930c397cad1b8a27d143a591ae57941b4d1f8c66823",
	"dumps-default.golden.json":     "dac79839e21dcc4e7d30e5f0292ac11c1ea2c3ca291faa872590125e16882f2c",
	"marshal.golden.json":           "07c3bb12bfbd04be5237c90af456a5d288912c74f62a0a7bb81d2cb7beb57c6d",
	"model.golden.json":             "1c03f955b30851bdb9cc7995e058e25cbd55c23ba5d774107b9631fe75bde205",
	"syntax-error-text.golden.json": "8eb43e4c76289c15dc869d93f4a9e142c648f6f1875d3797039691a7ddd0cbc4",
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
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
