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
	"body-int.golden.json":                "489acb16919046efc3d34cf6d6e70e399749b6f07f4b7aacf4c2ecbbc1328cb8",
	"date-and-aware-datetime.golden.json": "6d673713bdb9000f2813110cdde7a097b29c918b236e4f41c779d931b111ea7a",
	"email-str.golden.json":               "a13bbf8c10847e4ea3c3c7f2c25afb0c598f8e65b9b178ed2be2a0dc0cd1dcc0",
	"query-bool.golden.json":              "93b70d593c839b9a365c0acec7896cf63713a93f29636b64c3819f90bbc1b2fa",
	"query-int.golden.json":               "b4e201b66d52daf0b5de2cbaa48cf85a61745482cfb0cf1f0fdf17aa06caae9a",
	"query-uuid.golden.json":              "669a6094edf5df1935465ddc72dcf408a1470374361d3803604c427006783d5f",
	"string.golden.json":                  "2cd2c73d2906b69773146a545c731f9d72a66538efb6de1a4c90c339c189ee9b",
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
