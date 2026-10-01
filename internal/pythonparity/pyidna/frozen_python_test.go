package pyidna_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// idnaGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producer is the
// installed idna distribution and the interpreter's own "idna" codec, not a
// source file of this repository, so Identity names them.
// A golden recorded by another producer is refused.
var idnaGoldens = programoracle.Set{
	Package:       "./internal/pythonparity/pyidna/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nidna 3.20",
	Distributions: []string{"idna"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"behaviour.golden.json": "f55828ecda4e72e4d69babb0ad76fc481d25b89ada4c33c833472aa473df1c87",
		"codec.golden.json":     "26a367590e4eaeeb36b4d1d799d208855357df424c00de0fec6e2743d4827538",
		"tables.golden.json":    "82013d242e81a773a05cb0817a8c9da3dc338adc7ed546f5a61bcadfcd974d8f",
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
	return idnaGoldens.Outputs(t, root, golden, programs...)
}
