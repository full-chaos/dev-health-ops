package pyunicodedata_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// unicodeGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producer is the
// interpreter's unicodedata module, not a source file of this repository, so
// Identity names the interpreter and its Unicode data version.
// A golden recorded by another producer is refused.
var unicodeGoldens = programoracle.Set{
	Package:  "./internal/pythonparity/pyunicodedata/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"nfc.golden.json":    "b11baee9ba2ec856fec6e2f87298dd58598091877561b093531f23b91d370b72",
		"tables.golden.json": "e7188e845e07b6c4c2a4cd07fbc857e501194fd9f10efa0594f3720dc4d28030",
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
	return unicodeGoldens.Outputs(t, root, golden, programs...)
}
