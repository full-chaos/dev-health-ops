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
		"behaviour.golden.json": "93b5c472ff5a24f78361316db00d137b81fae0c58f9227f816d92650f9fc4c7c",
		"codec.golden.json":     "7b8d4babc5d978a05857218669e702c036a65b9671190577fefb308e5e296b79",
		"tables.golden.json":    "bbcae66d509029c29e39c60f1491b1f5f3de915208509cfdc8f94d0398c6ea8d",
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
