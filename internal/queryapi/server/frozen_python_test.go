package server

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// modelGoldens is the set of this package's frozen Python answers: each
// oracle program was executed once on Build, and its answer is frozen under
// testdata/golden. The producers are the query-api request and response
// models of that build on a FastAPI app, so Identity names FastAPI and the
// distributions under it. A golden recorded by another producer is refused.
var modelGoldens = programoracle.Set{
	Package:       "./internal/queryapi/server/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nfastapi 0.136.3\nhttpx 0.28.1\npydantic 2.13.5\npydantic-core 2.46.5\nstarlette 1.7.0",
	Distributions: []string{"fastapi", "httpx", "pydantic", "pydantic-core", "starlette"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"query-bodies.golden.json":                 "2b0614047c45642536c6ab0d64984b7f8a7dd5c4919a0294e551b545b1800879",
		"query-response-models-render.golden.json": "6f1b0c4b4e155ac2c02e9006db9f6d1325decd34934574372a47a8feccb408f1",
		"query-response-models-table.golden.json":  "e9c0e677cb4fc8ce4d10e51a6e96eb95e269c52894a3c069be4384b8bccfc9cf",
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
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return modelGoldens.Outputs(t, root, golden, programs...)
}
