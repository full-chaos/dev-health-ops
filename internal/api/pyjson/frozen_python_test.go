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
	"decode-body.golden.json":       "137806e607a7c7517e0e02629aae9e1feb32d8a0f823ef79743783836fcbda77",
	"dumps-default.golden.json":     "63b2147eb205253b4274d2c88fff413923922f2271692a38ae1d0ea3dd480862",
	"marshal.golden.json":           "57c8fd0a46e9ecc09c8ebc63f335e7db69c27f1b9d9d038d4c40c9fdeea26301",
	"model.golden.json":             "97e29f3e9a7f163c7801aac4496e9887bdc946daecf90af7ddd4dc6cbbb3f732",
	"syntax-error-text.golden.json": "3e190ffdc070267ed2f7289c254bb4eca0f923e17f16b54507723f889f6554b1",
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
