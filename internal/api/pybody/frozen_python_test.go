package pybody_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonBuild is the build whose interpreter answered the frozen oracles of
// this package: each program was executed there once.
const pythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// producerIdentity is the producer those answers came from. FastAPI,
// pydantic and the email validator are not source files of this repository, so the build above
// does not identify them: this does. A golden recorded by another interpreter
// or another version of one of these packages is refused.
const producerIdentity = "python 3.14.7\nunicodedata 16.0.0\nemail-validator 2.3.0\nfastapi 0.136.3\npydantic 2.13.5\npydantic_core 2.46.5\nstarlette 1.7.0"

// goldenPins pins the SHA-256 of every golden under testdata/golden. The
// goldenrecord verb writes each digest when it promotes a recording; a new
// golden starts as "PIN:" + its file name without ".json".
var goldenPins = map[string]string{
	"body-int.golden.json":                "ccc7fc5b10edb06b295ef8e62a54414aed1d5022000bc029a20c26372f91db98",
	"date-and-aware-datetime.golden.json": "6ea2077fa613f2c821644b880678b713fbe5ffaed1b96b68fc0c81755c4dd008",
	"email-str.golden.json":               "7951ade0cb254a844d6d07cd957445eeccec23fe6751d30b4b786ce7d72a6593",
	"query-bool.golden.json":              "0885cb6feb06897084c0fc132247fc855e7689db9fc38acee73e30206948f74f",
	"query-int.golden.json":               "d449b7224d219498cc933af2d8ea7fb4281a820e67a36bb376b5c154e85a100e",
	"query-uuid.golden.json":              "7094d8bc5ce1fccadee1e26afe9ebee726a13f26b2e25d22a745133543a584f4",
	"string.golden.json":                  "a8d977ed5488e8ac5ac7c3b0dac03b07129dd8606afbeaa85af3902dfae7ac9b",
}

// goldens is the set of this package's frozen Python answers.
var goldens = programoracle.Set{
	Package:       "./internal/api/pybody/",
	Build:         pythonBuild,
	Identity:      producerIdentity,
	Distributions: []string{"email-validator", "fastapi", "pydantic", "pydantic_core", "starlette"},
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
