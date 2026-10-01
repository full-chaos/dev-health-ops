package licensing

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// licensingGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producers are the licensing sources of that build and
// the distributions under them (the signature library, the payload model, the
// database layer of the two stored-row oracles), so Identity names those
// distributions. A golden recorded by another producer is refused.
var licensingGoldens = programoracle.Set{
	Package:       "./internal/api/licensing/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\naiosqlite 0.22.1\npydantic 2.13.5\npydantic-core 2.46.5\npynacl 1.6.2\nsqlalchemy 2.0.54",
	Distributions: []string{"aiosqlite", "pydantic", "pydantic-core", "pynacl", "sqlalchemy"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"b64decode.golden.json":            "7b19340f33067eccb9f67e8b15124aa3426021a777a3eb717bfb82c4fd1669f5",
		"big-duration-license.golden.json": "cdecd4e9d9adcdf905d477a134779d63cc86d9942b5a44138b579d780f1bbfc0",
		"org-feature.golden.json":          "db4500a13c5840437a90b9a997aff9b93f15a4b646afe0728ae5e6f4e13b590e",
		"sign-license.golden.json":         "7bc841f0200f457a23473a8fbecb5590035e652e766d1b6006e3b55c022af0ef",
		"tier-limits.golden.json":          "c759763a56dd38395987d00db987966985265f0055a36f66891c123dcec6456b",
		"tier-registry.golden.json":        "4557cdc49449cc6ee4bae33c7f78d08d533aad9e45caefa4a1f9f9c9ca45cafc",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs. A golden that is missing, edited,
// recorded for another program or input, or recorded by another producer
// fails the test; so does a program that exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return licensingGoldens.Outputs(t, root, golden, programs...)
}

// textDigest is how a golden holds a signed license: the SHA-256 of its text
// and the length of the text in bytes. A license is a signed token, and no
// golden holds a token.
type textDigest struct {
	SHA256 string `json:"sha256"`
	Length int    `json:"length"`
}
