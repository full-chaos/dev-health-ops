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
	"body-int.golden.json":                "2677e0cf2418973c993122a559ab9d7d8d91c1e82f1413b69b71a76648ec9468",
	"date-and-aware-datetime.golden.json": "3977dfd499647556c6429032ca379a6afe28517ae840b6e6c62125396f08ce51",
	"email-str.golden.json":               "44cf0c6f066b6b0914f74692d8dedf94402f9a920f368f1e9f54e19b828e3a49",
	"query-bool.golden.json":              "acc669ff07d489be7ef8de414597045c85a2bce7e39ab67abe4cf126afacd760",
	"query-int.golden.json":               "c3a58d6e35f1ece487f17277d6bca510124ef506869eabdcb3f087294219d926",
	"query-uuid.golden.json":              "8d11358e3279d13d9c59cc014ec1bad44288cf1de6440410a1b07975b24cfabd",
	"string.golden.json":                  "82c0f9e7274269ae00e5b10fc61f2add0c931558784829f7005dc26e9ecab8f8",
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
