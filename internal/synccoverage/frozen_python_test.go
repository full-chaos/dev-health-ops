package synccoverage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }

// coverageGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producer is the sync coverage service of that build
// and the database layer it imports, so Identity names those distributions. A
// golden recorded by another producer is refused.
var coverageGoldens = programoracle.Set{
	Package:       "./internal/synccoverage/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\npydantic 2.13.5\npydantic-core 2.46.5\nsqlalchemy 2.0.54",
	Distributions: []string{"pydantic", "pydantic-core", "sqlalchemy"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"effective-datasets.golden.json": "0e673a644cb8abf24bfa788dd0503d9e5f2daeae8c5342d75a7ea0b743bc9429",
		"payload.golden.json":            "d672d556633ba9fdced2bea99cd45231377f72b6a160272d23966f3505dd1da1",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return coverageGoldens.Outputs(t, root, golden, programs...)
}
