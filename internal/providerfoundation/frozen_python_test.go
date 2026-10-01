package providerfoundation_test

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
	"credential-field-grid.golden.json":        "65dd63db8a3aad124445b2315cf4b4288605313fb363a9f61b5cac568e5b95c5",
	"credential-field-grid-derive.golden.json": "b63edf0621072f9583264e92162e5371e1d28776e867876800159db0003bca51",
	"credential-field-reads.golden.json":       "b0bbdea306eef69a39080c98f85ba39dbc132e9b9f6e0a50f6dc781e8dfd366b",
	"fernet-custom-salt.golden.json":           "35ee080ae1e4ad46fbc73830e5df8d2bf68a6ee319c328b5cfc3067558be35db",
	"fernet-default-salt.golden.json":          "12c9132f337547e1d4e8516284713116b989ef7dbd31ee758eed5be762b9291b",
	"fernet-no-key.golden.json":                "0111efb8933a8b89830a68693b50c208822ec60b16e265be1cde60761b9f3c1a",
}

// goldens is the set of this package's frozen Python answers.
var goldens = programoracle.Set{
	Package:  "./internal/providerfoundation/",
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
