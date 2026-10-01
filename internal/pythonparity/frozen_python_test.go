package pythonparity_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// parityPythonBuild is the build whose interpreter answered the frozen oracles
// of this package: each program was executed there once.
const parityPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// parityProducerIdentity is the producer those answers came from. For most
// oracles of this package the producer is the interpreter itself (str.lower,
// repr, difflib, urllib, the UTF-8 codec), not a source file of this
// repository, so the build above does not identify it: this does. A golden
// recorded by another interpreter or another Unicode data version is refused.
const parityProducerIdentity = "python 3.14.7\nunicodedata 16.0.0"

// parityGoldenPins pins the SHA-256 of every golden under testdata/golden. The
// goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var parityGoldenPins = map[string]string{
	"casing-multi-rune.golden.json":       "4310cc31a85d42ca93e8bb6ef4d614d4a4c02deb5ecd63d376984dfda7fa80f5",
	"casing-sigma-distance.golden.json":   "3de72ac51892b9a287693cb70ac04d504413ee7accf19095ee060e7e63998190",
	"casing-sigma-properties.golden.json": "fbd0ed22057ecb8c49bbc024a8d6a30bfc2cf939ad22028764569e8e32e0495d",
	"errorsanitize.golden.json":           "f24ba04f425c388fdd33ff5714a14dff0fd19cfec58e915ef8efe509c6d7356b",
	"fnmatch.golden.json":                 "5a8b106fb0f27f55610af0eff8089d09b625081f8c901a66802ddc78d102330a",
	"isoformat.golden.json":               "e1fd71f57ff802c5413412c2d81e3c4bc188744d9c103dc4f5a31ba02e49e540",
	"seqratio.golden.json":                "a98a13c5da8408a4a5345e6780a80b03ceb0b4f41e4d8b4bc7df61cfe98effe3",
	"strrepr.golden.json":                 "a6b29a5054fa01c07ba9164c51e6c22cc3e1dd82dec5d52c31224a9e19500fde",
	"urlsplit.golden.json":                "26e28e8e05502b3d86b539701e7ca77d921853f32c483405ff52290435db3629",
	"utf8replace.golden.json":             "88be96ac3a79b38cea4d49c28a385206688bf4de196fa669aeb6ff5f413bf448",
}

// parityGoldens is the set of this package's frozen Python answers.
var parityGoldens = programoracle.Set{
	Package:  "./internal/pythonparity/",
	Build:    parityPythonBuild,
	Identity: parityProducerIdentity,
	Pins:     parityGoldenPins,
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs: the answers were executed once on
// parityPythonBuild and are frozen in testdata/golden. A golden that is
// missing, edited, recorded for another program or input, or recorded by
// another producer than parityProducerIdentity fails the test; so does a
// program that exited non-zero when it was recorded.
//
// The caller compares by value and sends no JSON number through float64.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	return parityGoldens.Outputs(t, frozenRepositoryRoot(t), golden, programs...)
}

// frozenRepositoryRoot is the repository root, from this file's own location.
func frozenRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
