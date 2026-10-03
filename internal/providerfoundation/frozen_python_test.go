package providerfoundation_test

import (
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
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
	"credential-field-grid.golden.json":        "c352036680cf5f75900bcde6ca706aad937b475bcc661dd6d4961005ca225bff",
	"credential-field-grid-derive.golden.json": "e71b84df68c7c93ccd58147ad918fb45b30d6fd460d57572ab5a5321be4820ca",
	"credential-field-reads.golden.json":       "a9e690ac1fd7f1cbdf04bab0311d627683a24876c540032cb1580ef9e1972a38",
	"fernet-custom-salt.golden.json":           "fdd075ff62501b1fe2d0ae493c9aa96b1ae6482569ebd4e68d2821b1f4fbb433",
	"fernet-default-salt.golden.json":          "a8bbc25e81c3b6a05836774c8cd32fe9ae093797f550f5a52c2946e471a5873d",
	"fernet-unconfigured.golden.json":          "bcf07875c4eeb49ecd2ebfe0ea0fe21594c4f903b68557fcee36f014625b4653",
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
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
