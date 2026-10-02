package syncbudget

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonBuild is the build whose interpreter answered the frozen oracles of
// this package: each program was executed there once.
const pythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// producerIdentity is the producer those answers came from: the interpreter
// and the installed packages the programs import are not source files of this
// repository, so the build above does not identify them. A golden recorded by
// another interpreter or another version of one of them is refused.
const producerIdentity = "python 3.14.7\nunicodedata 16.0.0"

// goldenPins pins the SHA-256 of every golden under testdata/golden. The
// goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var goldenPins = map[string]string{
	"budget-estimate.golden.json": "d07572e2d40dde72816819b3dd8619b4a0d9e03ab353bb3703ce01d854d191c0",
}

// goldens is the set of this package's frozen Python answers.
var goldens = programoracle.Set{
	Package:  "./internal/syncbudget/",
	Build:    pythonBuild,
	Identity: producerIdentity,
	Pins:     goldenPins,
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
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
