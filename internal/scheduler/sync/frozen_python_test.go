package sync

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// schedulerSyncGoldens is the set of this package's frozen Python answers:
// each oracle program was executed once on Build, over the planner, dataset,
// discovery and licensing sources of that build, and its answer is frozen
// under testdata/golden. Identity names the interpreter that ran them.
var schedulerSyncGoldens = programoracle.Set{
	Package:  "./internal/scheduler/sync/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"feature-decision-reason.golden.json": "55b285f7c70f2b7c7be3dbb571d1321ca8d8b2d0d310c8ca9ac4365c44e0f6c5",
		"jira-discovery-rows.golden.json":     "6d7e4b998503b7d49a3b8583a4fe423aaae7fc49817566c9a9e1e2d5da155480",
		"operator-datasets.golden.json":       "8732bb6691425b097b90d53b9d0b59037b0fa144f7ccd0e6d7ab47a908aa78e0",
		"planner-backfill.golden.json":        "597ddd354db215266932cc973ea60529b21f94bbff3043125727b95cb0204012",
		"planner-scheduled.golden.json":       "83a742a625d0b3d18db8900e67c3ceb73618fc201df0a51a15f1d12c98c0f95f",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs. A golden that is missing, edited,
// recorded for another program or input, or recorded by another interpreter
// fails the test; so does a program that exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return schedulerSyncGoldens.Outputs(t, root, golden, programs...)
}

// decodeKeepingNumbers decodes JSON with every number kept as its text, so a
// comparison of two decoded documents sends no number through float64.
func decodeKeepingNumbers(t *testing.T, text []byte, into any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(into); err != nil {
		t.Fatalf("decode: %v\n%s", err, text)
	}
}
